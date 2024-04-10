package cache_engine

import (
	"io"
	"os"
)

type StoreType int

const (
	TmpfsStoreStr  = "tmpfs"
	MemoryStoreStr = "memory"
)
const (
	FileType StoreType = iota
	MemoryType
)

var FileOpenOpt = os.O_CREATE | os.O_RDWR | os.O_EXCL

type Store interface {
	ReadAt(data []byte, offset int64) (int, error)
	WriteAt(data []byte, offset int64) (int, error)
	Close() error
}

type FileStore struct {
	fd *os.File
}

func NewFileStore(path string) (Store, error) {
	fd, err := os.OpenFile(path, FileOpenOpt, 0666)
	if err != nil {
		return nil, err
	}
	return &FileStore{fd: fd}, nil
}

func (s *FileStore) ReadAt(data []byte, offset int64) (int, error) {
	return s.fd.ReadAt(data, offset)
}

func (s *FileStore) WriteAt(data []byte, offset int64) (int, error) {
	return s.fd.WriteAt(data, offset)
}

func (s *FileStore) Close() error {
	if s.fd == nil {
		return nil
	}
	return s.fd.Close()
}

type MemoryStore struct {
	data []byte
}

func NewMemoryStore(size int64) Store {
	return &MemoryStore{
		data: make([]byte, size),
	}
}

func (s *MemoryStore) ReadAt(data []byte, offset int64) (int, error) {
	if int64(len(data))+offset > int64(len(s.data)) {
		return 0, io.EOF
	}
	copy(data, s.data[offset:offset+int64(len(data))])
	return len(data), nil
}

func (s *MemoryStore) WriteAt(data []byte, offset int64) (int, error) {
	if int64(len(data))+offset > int64(len(s.data)) {
		return 0, io.EOF
	}
	copy(s.data[offset:offset+int64(len(data))], data)
	return len(data), nil
}

func (s *MemoryStore) Close() error {
	//s.data = s.data[:0]
	return nil
}
