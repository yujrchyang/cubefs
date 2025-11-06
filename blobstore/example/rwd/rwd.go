package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/sdk"
)

const (
	objectNamePrefix = "blobstore_rwd_"
)

var (
	option      string
	dbDir       string
	config      string
	objectSize  string
	objectBytes uint64
	objectNum   int
	objectData  []byte
)

// toBytes parses a human-readable byte string (e.g., "2G", "512MiB") into uint64 bytes.
// It uses binary units (1K = 1024) by default.
// Supported suffixes: B, K/KiB, M/MiB, G/GiB, T/TiB, P/PiB (case-insensitive).
func toBytes(s string) (uint64, error) {
	if s == "" {
		return 0, errors.New("empty string")
	}

	// Trim whitespace
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty after trim")
	}

	// Find where the number ends and unit begins
	var i int
	for i = 0; i < len(s); i++ {
		c := s[i]
		if c == '.' || c == '+' || c == '-' || ('0' <= c && c <= '9') {
			continue
		}
		break
	}

	numPart := s[:i]
	unitPart := strings.TrimSpace(s[i:])

	// Parse number
	num, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q: %w", numPart, err)
	}

	if num < 0 {
		return 0, errors.New("negative size not allowed")
	}

	// Normalize unit
	unit := strings.ToUpper(unitPart)
	if unit == "" {
		unit = "B"
	}

	// Map unit to multiplier (binary: powers of 1024)
	var multiplier float64
	switch unit {
	case "B":
		multiplier = 1
	case "K", "KB", "KIB":
		multiplier = 1 << 10 // 1024
	case "M", "MB", "MIB":
		multiplier = 1 << 20 // 1024^2
	case "G", "GB", "GIB":
		multiplier = 1 << 30 // 1024^3
	case "T", "TB", "TIB":
		multiplier = 1 << 40 // 1024^4
	case "P", "PB", "PIB":
		multiplier = 1 << 50 // 1024^5
	default:
		// Try to handle cases like "Ki", "Mi" (without B)
		if len(unit) == 2 && unit[1] == 'I' && strings.ContainsRune("KMGTPE", rune(unit[0])) {
			switch unit[0] {
			case 'K':
				multiplier = 1 << 10
			case 'M':
				multiplier = 1 << 20
			case 'G':
				multiplier = 1 << 30
			case 'T':
				multiplier = 1 << 40
			case 'P':
				multiplier = 1 << 50
			default:
				return 0, fmt.Errorf("unknown unit %q", unitPart)
			}
		} else {
			return 0, fmt.Errorf("unknown unit %q", unitPart)
		}
	}

	result := num * multiplier
	if result > math.MaxUint64 {
		return 0, errors.New("size overflows uint64")
	}

	return uint64(result), nil
}

func checkParameters() {
	allowedOpt := map[string]struct{}{
		"wrd": {},
	}
	if _, ok := allowedOpt[option]; !ok {
		log.Fatalf("invalid option: %s", option)
	}
	if config == "" {
		log.Fatalf("please input sdk config file")
	}
	info, err := os.Stat(config)
	if err != nil {
		log.Fatalf("failed to get config file(%s) stat info: %v", config, err)
	}
	if info.IsDir() {
		log.Fatalf("config file(%s) is a directory", config)
	}
	if s, err := toBytes(objectSize); err != nil {
		log.Fatalf("failed to convert objectSize(%s): %v", objectSize, err)
	} else {
		objectBytes = s
	}
	if objectBytes <= 0 {
		log.Fatalf("invalid objectSize: %s", objectSize)
	}
	if objectNum <= 0 {
		log.Fatalf("invalid objectNum: %d", objectNum)
	}
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		log.Fatalf("failed to create db dir(%s): %v", dbDir, err)
	}
}

func prepareObjectData() {
	objectData = make([]byte, objectBytes)
	rand.Read(objectData)
	hasher := sha256.New()
	hasher.Write(objectData)
	_ = base64.StdEncoding.EncodeToString(hasher.Sum(nil))
}

func init() {
	flag.StringVar(&option, "o", "wrd", "The action you want to perform")
	flag.StringVar(&dbDir, "d", "/tmp/blobstore_rwd_test", "Rocksdb database directory")
	flag.StringVar(&config, "c", "", "SDK configuration file path")
	flag.StringVar(&objectSize, "s", "1M", "The size of the object with postfix K, M, and G")
	flag.IntVar(&objectNum, "n", 1, "The number of objects you want to write")
	flag.Parse()

	checkParameters()
	prepareObjectData()
}

type RWDStore struct {
	ctx    context.Context
	client access.API
}

func (s *RWDStore) putObject(data io.Reader, size int64) (loc proto.Location, err error) {
	args := &access.PutArgs{
		Size:   size,
		Hashes: access.HashAlgDummy,
		Body:   data,
	}

	loc, _, err = s.client.Put(s.ctx, args)
	if err != nil {
		return proto.Location{}, err
	}

	return loc, nil
}

func (s *RWDStore) getObject(loc proto.Location, size int64) error {
	args := &access.GetArgs{
		Location: loc,
		Offset:   0,
		ReadSize: uint64(size),
	}

	rc, err := s.client.Get(s.ctx, args)
	if err != nil {
		return err
	}
	defer rc.Close()

	// Write data to the provided io.Writer
	writer := io.Discard
	_, err = io.Copy(writer, rc)
	if err != nil {
		return err
	}

	return nil
}

func (s *RWDStore) delObject(loc proto.Location) error {
	args := &access.DeleteArgs{
		Locations: []proto.Location{loc},
	}
	_, err := s.client.Delete(s.ctx, args)
	return err
}

func (s *RWDStore) doWriteReadDelete() {
	obj := bytes.NewReader(objectData)

	for i := 1; i <= objectNum; i++ {
		objName := fmt.Sprintf("%s%d", objectNamePrefix, i)
		loc, err := s.putObject(obj, obj.Size())
		if err != nil {
			log.Fatalf("failed to put object(%s) to blobstore: %v", objName, err)
		}

		err = s.getObject(loc, obj.Size())
		if err != nil {
			log.Fatalf("failed to get object(%s) from blobstore: %v", objName, err)
		}

		err = s.delObject(loc)
		if err != nil {
			log.Fatalf("failed to del object(%s) from blobstore: %v", objName, err)
		}
	}
}

func initRWDStore() *RWDStore {
	// init context
	ctx := context.Background()

	// init BlobStore client
	configData, err := os.ReadFile(config)
	if err != nil {
		log.Fatalf("failed to read config file(%s): %v", config, err)
	}
	var conf sdk.Config
	if err := json.Unmarshal(configData, &conf); err != nil {
		log.Fatalf("failed to parse config file(%s): %v", config, err)
	}
	client, err := sdk.New(&conf)
	if err != nil {
		log.Fatalf("failed to create blobstore client: %v", err)
	}

	return &RWDStore{
		ctx:    ctx,
		client: client,
	}
}

func main() {
	rwdClient := initRWDStore()

	switch option {
	case "wrd":
		rwdClient.doWriteReadDelete()
	default:
		log.Fatalf("invalid option: %s", option)
	}
}
