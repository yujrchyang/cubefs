package cutil

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cubefs/cubefs/util/log"
)

var CFS_S3 *ConsoleS3 // 用来支持console的文件下载
const presignUrlExpire = time.Minute * 20

type ConsoleS3 struct {
	s3Client   *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
	cfg        *S3Config
}

func InitConsoleS3(config *S3Config) (*ConsoleS3, error) {
	sdkConfig := aws.NewConfig()
	sdkConfig.BaseEndpoint = aws.String(config.EndPoint)
	sdkConfig.Credentials = aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(config.AccessKey, config.SecretKey, ""))
	s3Client := s3.NewFromConfig(*sdkConfig)
	downloader := manager.NewDownloader(s3Client)
	upload := manager.NewUploader(s3Client, func(u *manager.Uploader) {
		u.PartSize = 64 * 1024 * 1024 // 64MB per part
	})
	return &ConsoleS3{
		s3Client:   s3Client,
		downloader: downloader,
		uploader:   upload,
		cfg:        config,
	}, nil
}

func (s *ConsoleS3) GetS3Client() *s3.Client {
	return s.s3Client
}

func (s *ConsoleS3) Upload(filename string, file io.Reader) (err error) {
	if _, err = s.uploader.Upload(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(filename),
		Body:   file,
	}); err != nil {
		log.LogErrorf("console Upload: filename(%v) err(%v)", file, err)
	}
	return
}

// 一次写入，不支持追加
func (s *ConsoleS3) GetDownloadPresignUrl(filename string, file io.Reader) (signUrl string, err error) {
	err = s.Upload(filename, file)
	if err != nil {
		return
	}
	client := s.GetS3Client()
	downloadOp := &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(filename),
	}
	presignClient := s3.NewPresignClient(client)
	request, err := presignClient.PresignGetObject(context.Background(),
		downloadOp,
		func(opts *s3.PresignOptions) {
			opts.Expires = presignUrlExpire
		})
	if err != nil {
		log.LogErrorf("console Presign URL: filename(%v) err(%v)", filename, err)
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("GetDownloadPresignUrl success: url(%v)", request.URL)
	}
	return request.URL, nil
}

func CalcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}
