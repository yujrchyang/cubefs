package data

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
)

const (
	readAheadChanCap 	= 102400
	readAheadBlockSize 	= uint64(128 * unit.KB)

	maxReadAheadWorkerNum    	= 128
	maxReadAheadMemMB  			= 8192
	maxReadAheadWindowMB  		= 64
	defaultReadAheadWindowMB	= 2

	readAheadPrintInterval		= 60 * time.Second
	readAheadEvictInterval		= 1 * time.Second
	readAheadEvictExpiredSec	= 5
)

type ReadAheadController struct {
	volumeName			string
	windowSize			uint64
	blockCntThreshold	int64
	blockCount			int64
	blockLruList		*list.List
	listMutex			sync.RWMutex
	prepareChan			chan *ReadAheadBlock
	blockChan   		chan *ReadAheadBlock
	stopC				chan struct{}
	wg          		sync.WaitGroup
	getStreamer 		func(inode uint64) *Streamer

	localMemMB			int64
	localWindowMB		int64
	remoteMemMB			int64
	remoteWindowMB		int64
}

type ReadAheadBlock struct {
	inodeID         uint64
	startFileOffset uint64
	endFileOffset   uint64
	data           	[]byte
	actualDataSize	uint64
	hasHole			bool
	timestamp      	int64
	elem			*list.Element

	mu	sync.RWMutex
}

func (b *ReadAheadBlock) String() string {
	if b == nil {
		return ""
	}
	return fmt.Sprintf("Inode(%v) StartFileOffset(%v) EndFileOffset(%v) ActualSize(%v) Timestamp(%v)",
		b.inodeID, b.startFileOffset, b.endFileOffset, b.actualDataSize, b.timestamp)
}

func NewReadAheadController(wrapper *Wrapper, memoryMB int64, readAheadWindowSize uint64) (controller *ReadAheadController, err error) {
	if wrapper.getStreamerFunc == nil {
		err = fmt.Errorf("empty func 'GetStreamer'")
		return nil, err
	}
	blockThreshold := memoryMB * unit.MB / int64(readAheadBlockSize)
	controller = &ReadAheadController{
		volumeName: 		wrapper.volName,
		windowSize:			readAheadWindowSize,
		blockCntThreshold: 	blockThreshold,
		blockCount:        	0,
		blockLruList:      	list.New(),
		prepareChan:       	make(chan *ReadAheadBlock, readAheadChanCap),
		blockChan:         	make(chan *ReadAheadBlock, readAheadChanCap),
		stopC:  			make(chan struct{}),
		getStreamer: 		wrapper.getStreamerFunc,
	}
	workerNum := unit.Min(2 * runtime.NumCPU(), maxReadAheadWorkerNum)
	for i := 0; i < workerNum; i++ {
		controller.wg.Add(1)
		go controller.ReadAheadWorker()
	}
	controller.wg.Add(1)
	go controller.CleanExpiredBlock()

	log.LogInfof("init read ahead controller: memoryMB(%v) blockThreshold(%v) workerNum(%v)", memoryMB, blockThreshold, workerNum)
	return controller, nil
}

func (c *ReadAheadController) ReadAheadWorker() {
	defer func() {
		if r := recover(); r != nil {
			stack := fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			log.LogErrorf("ReadAheadWorker panic: %s\n", stack)
		}
		c.wg.Done()
	}()

	ctx := context.Background()
	for {
		select {
		case prepareInfo := <- c.prepareChan:
			if log.IsDebugEnabled() {
				log.LogDebugf("read ahead prepare: %v", prepareInfo)
			}
			c.splitReadAheadBlock(prepareInfo)

		case block := <- c.blockChan:
			if log.IsDebugEnabled() {
				log.LogDebugf("begin read ahead block: %v", block)
			}
			start := time.Now()
			streamer := c.getStreamer(block.inodeID)
			if streamer == nil {
				continue
			}
			if value, ok := streamer.readAheadBlocks.Load(block.startFileOffset); ok && value.(*ReadAheadBlock).actualDataSize > 0 {
				continue
			}
			if err := c.doReadAhead(ctx, streamer, block); err != nil {
				log.LogWarnf("read ahead err: %v, block(%v)", err, block)
				continue
			}
			if log.IsDebugEnabled() {
				log.LogDebugf("end read ahead block: %v, cost(%v)", block, time.Since(start))
			}

		case <- c.stopC:
			return
		}
	}
}

func (c *ReadAheadController) CleanExpiredBlock() {
	evictTicker := time.NewTicker(readAheadEvictInterval)
	printTicker := time.NewTicker(readAheadPrintInterval)

	defer func() {
		if r := recover(); r != nil {
			stack := fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
			log.LogErrorf("CleanExpiredBlock panic: %s\n", stack)
		}
		evictTicker.Stop()
		printTicker.Stop()
		c.wg.Done()
	}()

	for {
		select {
		case <- evictTicker.C:
			c.evictExpiredBlock()

		case <- printTicker.C:
			c.listMutex.RLock()
			listLen := c.blockLruList.Len()
			c.listMutex.RUnlock()

			log.LogInfof("read ahead list length(%v) block count(%v)", listLen, atomic.LoadInt64(&c.blockCount))

		case <- c.stopC:
			return
		}
	}
}

func (c *ReadAheadController) Stop() {
	if c == nil {
		return
	}
	close(c.stopC)
	c.wg.Wait()
}

func (c *ReadAheadController) ReadFromBlocks(s *Streamer, data []byte, offset, size uint64) (read int, hole bool, err error) {
	if c == nil || atomic.LoadInt64(&c.blockCntThreshold) == 0 {
		return
	}
	windowSize := atomic.LoadUint64(&c.windowSize)
	if inodeSize, _ := s.extents.Size(); inodeSize < 2 * windowSize {
		return
	}
	tpObject := exporter.NewVolumeTPUs("ReadAhead_us", c.volumeName)
	defer func() {
		tpObject.Set(err)
		if err != nil {
			if log.IsDebugEnabled() {
				//	log.LogDebugf("ino(%v) offset(%v) size(%v) read ahead failed: %v", s.inode, offset, size, err)
			}
		}
	}()
	// 第一次落在窗口区间时，触发后一窗口预读
	if (offset % windowSize) < 128 * unit.KB {
		_ = c.prepareReadAhead(s, offset)
	}
	readOffset := offset

	s.readAheadLock.RLock()
	defer s.readAheadLock.RUnlock()

	for readAheadBlockStartOffset := offset/readAheadBlockSize * readAheadBlockSize; readAheadBlockStartOffset < offset + size; readAheadBlockStartOffset += readAheadBlockSize {
		if readOffset < readAheadBlockStartOffset {
			err = fmt.Errorf("insufficient data at last read ahead block offset(%v) curReadOffset(%v)", readAheadBlockStartOffset, readOffset)
			return
		}
		value, ok := s.readAheadBlocks.Load(readAheadBlockStartOffset)
		if !ok {
			err = fmt.Errorf("no block at read ahead offset(%v)", readAheadBlockStartOffset)
			return
		}
		readBlockOffset := readOffset - readAheadBlockStartOffset
		block := value.(*ReadAheadBlock)
		block.mu.RLock()
		blkSize := block.actualDataSize
		if blkSize == 0 || blkSize <= readBlockOffset || block.inodeID != s.inode || block.startFileOffset != readAheadBlockStartOffset {
			block.mu.RUnlock()
			err = fmt.Errorf("no data at read ahead offset(%v)", readAheadBlockStartOffset)
			return
		}
		readBlockSize := unit.MinUint64(blkSize-readBlockOffset, size-uint64(read))
		copy(data[(readOffset-offset):(readOffset-offset+readBlockSize)], block.data[readBlockOffset:(readBlockOffset+readBlockSize)])
		hole = hole || block.hasHole
		block.mu.RUnlock()
		if log.IsDebugEnabled() {
			log.LogDebugf("ino(%v) offset(%v) size(%v) read ahead offset(%v) size(%v)", s.inode, offset, size, readOffset, readBlockSize)
		}
		readOffset += readBlockSize
		read += int(readBlockSize)
		if readBlockSize + readBlockOffset == blkSize {
			s.readAheadBlocks.Delete(readAheadBlockStartOffset)
			c.removeBlockFromQueue(block)
			c.putBlockData(block)
		}
		if read >= int(size) {
			return
		}
	}
	return
}

func (c *ReadAheadController) EvictBlocksAtOffset(s *Streamer, offset uint64, size int) {
	satisfyConditionFunc := func(block *ReadAheadBlock) bool {
		if block.startFileOffset + block.actualDataSize <= offset || block.startFileOffset >= offset + uint64(size) {
			return false
		}
		return true
	}
	c.evictBlocksByCondition(s, satisfyConditionFunc)
}

func (c *ReadAheadController) EvictBlocksFromOffset(s *Streamer, offset uint64) {
	satisfyConditionFunc := func(block *ReadAheadBlock) bool {
		if block.startFileOffset + block.actualDataSize <= offset {
			return false
		}
		return true
	}
	c.evictBlocksByCondition(s, satisfyConditionFunc)
}

func (c *ReadAheadController) evictBlocksByCondition(s *Streamer, satisfyConditionFunc func(block *ReadAheadBlock) bool) {
	if c == nil {
		return
	}
	s.readAheadLock.Lock()
	defer s.readAheadLock.Unlock()

	s.readAheadBlocks.Range(func(key, value interface{}) bool {
		block := value.(*ReadAheadBlock)
		if !satisfyConditionFunc(block) {
			return true
		}
		if log.IsDebugEnabled() {
			log.LogDebugf("ino(%v) evict block(%v) after write or truncate", s.inode, block)
		}
		s.readAheadBlocks.Delete(key)
		c.removeBlockFromQueue(block)
		c.putBlockData(block)
		return true
	})
}

func (c *ReadAheadController) prepareReadAhead(s *Streamer, offset uint64) (preparing bool) {
	windowSize := atomic.LoadUint64(&c.windowSize)
	inodeSize, _ := s.extents.Size()
	readAheadStartOffset := (offset/windowSize + 1) * windowSize
	if readAheadStartOffset >= inodeSize {
		return false
	}
	readAheadEndOffset := unit.MinUint64(inodeSize, readAheadStartOffset + windowSize)
	if offset/windowSize == 0 {
		readAheadStartOffset = 128 * unit.KB
	}
	inodeReadAhead := &ReadAheadBlock{inodeID: s.inode, startFileOffset: readAheadStartOffset, endFileOffset: readAheadEndOffset}
	// 占位，避免重复预读
	if _, loaded := s.readAheadBlocks.LoadOrStore(readAheadStartOffset, inodeReadAhead); !loaded {
		select {
		case c.prepareChan <- inodeReadAhead:
			if log.IsDebugEnabled() {
				log.LogDebugf("block(%v) put to prepare channel", inodeReadAhead)
			}
		default:
			log.LogWarnf("read ahead prepare channel is full, failed prepare (%v)", inodeReadAhead)
		}
		return true
	}
	return false
}

func (c *ReadAheadController) splitReadAheadBlock(prepareInfo *ReadAheadBlock) {
	for readOffset := prepareInfo.startFileOffset; readOffset < prepareInfo.endFileOffset; readOffset += readAheadBlockSize {
		blockStartOffset := readOffset
		blockEndOffset := unit.MinUint64(prepareInfo.endFileOffset, readOffset+readAheadBlockSize)
		select {
		case c.blockChan <- &ReadAheadBlock{inodeID: prepareInfo.inodeID, startFileOffset: blockStartOffset, endFileOffset: blockEndOffset}:
		default:
			log.LogWarnf("read ahead block channel is full, failed prepare (%v)", prepareInfo)
			return
		}
	}
}

func (c *ReadAheadController) doReadAhead(ctx context.Context, streamer *Streamer, block *ReadAheadBlock) (err error) {
	defer func() {
		if err != nil {
			// 移除占位block
			if value, ok := streamer.readAheadBlocks.Load(block.startFileOffset); ok && value.(*ReadAheadBlock).actualDataSize == 0 {
				streamer.readAheadBlocks.Delete(block.startFileOffset)
			}
		}
	}()

	if getErr := c.getBlockData(block); getErr != nil {
		c.putBlockData(block)
		err = fmt.Errorf("get block data(%v) err: %v", readAheadBlockSize, getErr)
		return
	}
	readOffset := block.startFileOffset
	readSize := block.endFileOffset - block.startFileOffset
	var (
		totalRead 	int
		hasHole		bool
		readErr		error
	)
	streamer.readAheadLock.RLock()
	totalRead, hasHole, readErr = streamer.read(ctx, block.data, readOffset, int(readSize))
	streamer.readAheadLock.RUnlock()
	if (readErr != nil && readErr != io.EOF) || totalRead == 0 || totalRead > int(readAheadBlockSize) {
		c.putBlockData(block)
		err = fmt.Errorf("read data offset(%v) size(%v) err: %v, totalRead(%v)", readOffset, readSize, readErr, totalRead)
		return
	}
	block.mu.Lock()
	block.actualDataSize = uint64(totalRead)
	block.hasHole = hasHole
	block.timestamp = time.Now().Unix()
	block.mu.Unlock()

	c.enqueueBlock(block)
	streamer.readAheadBlocks.Store(readOffset, block)
	return
}

func (c *ReadAheadController) ClearInodeBlocks(streamer *Streamer) {
	if c == nil {
		return
	}
	count := 0
	streamer.readAheadBlocks.Range(func(key, value interface{}) bool {
		count++
		block := value.(*ReadAheadBlock)
		streamer.readAheadBlocks.Delete(key)
		if block.elem != nil {
			c.removeBlockFromQueue(block)
			c.putBlockData(block)
		}
		return true
	})
	if log.IsDebugEnabled() {
		log.LogDebugf("ino(%v) clear read ahead blocks total(%v)", streamer.inode, count)
	}
}

func (c *ReadAheadController) getBlockData(block *ReadAheadBlock) (err error) {
	atomic.AddInt64(&c.blockCount, 1)
	c.evictExcessBlock()
	block.data, err = proto.Buffers.Get(int(readAheadBlockSize))
	if err != nil {
		atomic.AddInt64(&c.blockCount, -1)
	}
	return
}

func (c *ReadAheadController) putBlockData(block *ReadAheadBlock) {
	block.mu.Lock()
	if len(block.data) == 0 {
		block.mu.Unlock()
		return
	}
	proto.Buffers.Put(block.data)
	block.actualDataSize = 0
	block.data = nil
	block.mu.Unlock()

	atomic.AddInt64(&c.blockCount, -1)
}

func (c *ReadAheadController) enqueueBlock(block *ReadAheadBlock) {
	c.listMutex.Lock()
	defer c.listMutex.Unlock()

	block.elem = c.blockLruList.PushBack(block)
}

func (c *ReadAheadController) dequeueBlock(conditionFunc func(blk *ReadAheadBlock) bool) *ReadAheadBlock {
	c.listMutex.Lock()
	defer c.listMutex.Unlock()

	elem := c.blockLruList.Front()
	if elem == nil {
		return nil
	}
	block := elem.Value.(*ReadAheadBlock)
	if !conditionFunc(block) {
		return nil
	}
	c.blockLruList.Remove(elem)
	return block
}

func (c *ReadAheadController) removeBlockFromQueue(block *ReadAheadBlock) {
	c.listMutex.Lock()
	defer c.listMutex.Unlock()

	if block.elem != nil {
		c.blockLruList.Remove(block.elem)
	}
}

func (c *ReadAheadController) evictExcessBlock() {
	for atomic.LoadInt64(&c.blockCount) > atomic.LoadInt64(&c.blockCntThreshold) {
		block := c.dequeueBlock(func(blk *ReadAheadBlock) bool {
			if atomic.LoadInt64(&c.blockCount) > atomic.LoadInt64(&c.blockCntThreshold) {
				return true
			}
			return false
		})
		if block == nil {
			return
		}
		if log.IsDebugEnabled() {
			log.LogDebugf("evict excess block(%v)", block)
		}
		streamer := c.getStreamer(block.inodeID)
		if streamer != nil {
			streamer.readAheadBlocks.Delete(block.startFileOffset)
		}
		c.putBlockData(block)
	}
}

func (c *ReadAheadController) evictExpiredBlock() {
	for {
		block := c.dequeueBlock(func(blk *ReadAheadBlock) bool {
			if time.Now().Unix() - blk.timestamp >= readAheadEvictExpiredSec {
				return true
			}
			return false
		})
		if block == nil {
			return
		}
		if log.IsDebugEnabled() {
			log.LogDebugf("evict expired block(%v)", block)
		}
		streamer := c.getStreamer(block.inodeID)
		if streamer != nil {
			streamer.readAheadBlocks.Delete(block.startFileOffset)
		}
		c.putBlockData(block)
	}
}

func (c *ReadAheadController) updateBlockCntThreshold(memoryMB int64) {
	if c == nil {
		return
	}
	newBlockThreshold := int64(0)
	if memoryMB > 0 {
		newBlockThreshold = memoryMB * unit.MB / int64(readAheadBlockSize)
	}
	if c.blockCntThreshold != newBlockThreshold {
		log.LogInfof("update memoryMB to (%v)MB, block threshold from (%v) to (%v)", memoryMB, c.blockCntThreshold, newBlockThreshold)
		atomic.StoreInt64(&c.blockCntThreshold, newBlockThreshold)
	}
}

func (c *ReadAheadController) updateWindowSize(windowMB int64) {
	if c == nil {
		return
	}
	newWindowSize := uint64(defaultReadAheadWindowMB) * unit.MB
	if windowMB > 0 {
		newWindowSize = uint64(windowMB) * unit.MB
	}
	if c.windowSize != newWindowSize {
		log.LogInfof("update windowSize to (%v)", newWindowSize)
		atomic.StoreUint64(&c.windowSize, newWindowSize)
	}
}

func (c *ReadAheadController) getMemoryMB() int64 {
	if c == nil {
		return 0
	}
	blkThreshold := atomic.LoadInt64(&c.blockCntThreshold)
	return blkThreshold * int64(readAheadBlockSize) / unit.MB
}

func (c *ReadAheadController) getWindowMB() int64 {
	if c == nil {
		return 0
	}
	windowSize := atomic.LoadUint64(&c.windowSize)
	return int64(windowSize) / unit.MB
}

func validateReadAheadConfig(readAheadMemMB, readAheadWindowMB int64) (err error) {
	if readAheadMemMB > maxReadAheadMemMB {
		err = fmt.Errorf("invalid read ahead memory MB: %v, out of [-1~%v]MB", readAheadMemMB, maxReadAheadMemMB)
		return
	}
	if readAheadWindowMB > maxReadAheadWindowMB {
		err = fmt.Errorf("invalid read ahead window MB: %v, out of [-1~%v]MB", readAheadWindowMB, maxReadAheadWindowMB)
		return
	}
	return
}