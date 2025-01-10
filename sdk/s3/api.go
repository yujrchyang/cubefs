package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/cubefs/cubefs/util/log"
)

const (
	RetryModeStandard aws.RetryMode = "standard"
	RetryModeAdaptive aws.RetryMode = "adaptive"

	DefaultMaxAttempts = retry.DefaultMaxAttempts
)

type S3Client struct {
	s3Client *s3.Client
}

func NewS3Client(region, endPoint, ak, sk string, useSSL bool) *S3Client {
	return &S3Client{
		s3Client: newS3ClientFromConfig(region, endPoint, ak, sk, useSSL, DefaultMaxAttempts, RetryModeStandard),
	}
}

func NewS3ClientWithRetry(region, endPoint, ak, sk string, useSSL bool, retryMaxAttempts int, retryMode aws.RetryMode) *S3Client {
	return &S3Client{
		s3Client: newS3ClientFromConfig(region, endPoint, ak, sk, useSSL, retryMaxAttempts, retryMode),
	}
}

func newS3ClientFromConfig(region, endPoint, ak, sk string, useSSL bool, retryMaxAttempts int, retryMode aws.RetryMode) *s3.Client {
	return s3.NewFromConfig(
		aws.Config{
			RetryMaxAttempts:	retryMaxAttempts,
			RetryMode: 		  	retryMode,
		},
		func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endPoint)
		},
		func(o *s3.Options) {
			o.Credentials = credentials.NewStaticCredentialsProvider(ak, sk, "")
		},
		func(o *s3.Options) {
			o.Region = region
		},
		func(o *s3.Options) {
			if !useSSL {
				o.UsePathStyle = true
			}
		})
}

func (c *S3Client) CheckBucketExist(ctx context.Context, bucketName string) (exist bool, err error) {
	headBucketInput := &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}

	_, err = c.s3Client.HeadBucket(ctx, headBucketInput)
	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				log.LogErrorf("Bucket %v is unavailable.", bucketName)
				exist = false
				err = nil
			default:
				fmt.Printf("errorCode: %v, errMsg: %v\n", apiError.ErrorCode(), apiError.ErrorMessage())
				log.LogErrorf("Either you don't have access to bucket %v or another error occurred. "+
					"Here's what happened: %v", bucketName, err)
			}
		}
		return
	}

	exist = true
	err = nil
	return
}

func (c *S3Client) HeadObject(ctx context.Context, bucketName, key string) (exist bool, err error) {
	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key: aws.String(key),
	}
	_, err = c.s3Client.HeadObject(ctx, headObjectInput)
	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				log.LogErrorf("object %v not exist in %s.", key, bucketName)
				exist = false
				err = nil
			default:
				fmt.Printf("errorCode: %v, errMsg: %v\n", apiError.ErrorCode(), apiError.ErrorMessage())
				log.LogErrorf("Either you don't have access to bucket %v or another error occurred. "+
					"Here's what happened: %v", bucketName, err)
			}
		}
	}
	exist = true
	return
}

func (c *S3Client) PutObject(ctx context.Context, bucketName, key string, data []byte) (err error) {
	putObjectInput := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}
	_, err = c.s3Client.PutObject(ctx, putObjectInput)
	if err != nil {
		log.LogErrorf("PutObject failed, bucket: %s, key: %s, err: %v", bucketName, key, err)
	}
	return
}

func (c *S3Client) GetObject(ctx context.Context, bucketName, key string, offset, size uint64, data []byte) (readN int, err error) {
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)),
	}
	var getObjectOutput *s3.GetObjectOutput
	getObjectOutput, err = c.s3Client.GetObject(ctx, getObjectInput)
	if err != nil {
		log.LogErrorf("")
		return
	}
	defer getObjectOutput.Body.Close()

	var countPerRead, readOffset int
	for {
		countPerRead, err = getObjectOutput.Body.Read(data[readOffset:])
		readN += countPerRead
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return 0, err
		}

		readOffset += countPerRead
	}
	return
}

func (c *S3Client) GetObjectContentLength(ctx context.Context, bucketName, key string) (contentLength int64, err error) {
	headObjectInput := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	result, err := c.s3Client.HeadObject(ctx, headObjectInput)
	if err != nil {
		return 0, err
	}
	contentLength = result.ContentLength
	return
}

func (c *S3Client) DeleteObject(ctx context.Context, bucketName, key string) (err error) {
	deleteObjectInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	_, err = c.s3Client.DeleteObject(ctx, deleteObjectInput)
	if err != nil {
		log.LogErrorf("DeleteObject failed, bucket: %s, key: %s, err: %v", bucketName, key, err)
	}
	return
}

func (c *S3Client) BatchDeleteObject(ctx context.Context, bucketName string, keys []string) (err error) {
	if len(keys) == 0 {
		return
	}

	objectsToDelete := make([]types.ObjectIdentifier, 0, len(keys))
	for _, key := range keys {
		objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
			Key: aws.String(key),
		})
	}

	_, err = c.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{
			Objects: objectsToDelete,
		},
	})
	if err != nil {
		log.LogErrorf("BatchDeleteObject delete objects failed, bucket: %s, keys: %s, err: %v", bucketName, keys, err)
	}
	return
}

func (c *S3Client) UploadByPart(ctx context.Context, bucketName, key string, partCount int, numOfParallel int, f func(index int) (data []byte, err error)) (err error) {
	var createResp *s3.CreateMultipartUploadOutput
	createResp, err = c.s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		log.LogErrorf("UploadByPart CreateMultipartUpload failed, bucket: %s, key: %s, err: %v", bucketName, key, err)
		return
	}

	var uploadID = createResp.UploadId
	defer func() {
		if err != nil {
			_, err = c.s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:              aws.String(bucketName),
				Key:                 aws.String(key),
				UploadId:            uploadID,
				ExpectedBucketOwner: nil,
				RequestPayer:        "",
			})
			if err != nil {
				log.LogErrorf("UploadByPart AbortMultipartUpload failed, bucket: %s, key: %s, err: %v", bucketName, key, err)
			}
		}
	}()

	var parts = make([]types.CompletedPart, partCount)
	var wg sync.WaitGroup
	var parallelChan = make(chan struct{}, numOfParallel)
	var errCh = make(chan error, partCount)
	for i := 0; i < partCount; i++ {
		wg.Add(1)
		parallelChan <- struct{}{}
		go func(i int) {
			defer func() {
				wg.Done()
				<-parallelChan
			}()
			var (
				uploadResp    *s3.UploadPartOutput
				errUploadPart error
				data          []byte
			)
			data, errUploadPart = f(i)
			if errUploadPart != nil {
				errCh <- errUploadPart
				log.LogErrorf("UploadByPart get data failed, bucket: %s, key: %s, partNum: %v, err: %v", bucketName, key, i+1, err)
				return
			}
			uploadResp, errUploadPart = c.s3Client.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(bucketName),
				Key:        aws.String(key),
				PartNumber: int32(i) + 1,
				UploadId:   uploadID,
				Body:       bytes.NewReader(data),
			})
			if errUploadPart != nil {
				errCh <- errUploadPart
				log.LogErrorf("UploadByPart UploadPart failed, bucket: %s, key: %s, partNum: %v, err: %v", bucketName, key, i+1, err)
				return
			}

			parts[i] = types.CompletedPart{
				ETag:       uploadResp.ETag,
				PartNumber: int32(i) + 1,
			}
		}(i)
	}
	wg.Wait()

	select {
	case err = <-errCh:
		return
	default:
	}

	_, err = c.s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String(key),
		UploadId:        uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		log.LogErrorf("UploadByPart CompleteMultipartUpload failed, bucket: %s, key: %s, err: %v", bucketName, key, err)
	}
	return
}

func (c *S3Client) GetObjectsCountByPrefix(ctx context.Context, bucketName, keyPrefix string) (count int, err error) {
	var listResp *s3.ListObjectsOutput
	var marker *string

	for {
		listResp, err = c.s3Client.ListObjects(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
			Prefix: aws.String(keyPrefix),
			Marker: marker,
		})
		if err != nil || listResp == nil {
			log.LogErrorf("ListObject failed, bucket: %s, keyPrefix: %v, err: %v", bucketName, keyPrefix, err)
			break
		}

		count += len(listResp.Contents)

		if !listResp.IsTruncated {
			break
		}

		marker = listResp.NextMarker
	}
	return
}

func (c *S3Client) DeleteObjectsByPrefix(ctx context.Context, bucketName, keyPrefix string) (err error) {
	var listResp *s3.ListObjectsOutput
	var maker *string
	var objectsToDelete []types.ObjectIdentifier

	for {
		listResp, err = c.s3Client.ListObjects(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
			Prefix: aws.String(keyPrefix),
			Marker: maker,
		})
		if err != nil || listResp == nil {
			log.LogErrorf("ListObject failed, bucket: %s, keyPrefix: %v, err: %v", bucketName, keyPrefix, err)
			break
		}

		for _, object := range listResp.Contents {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{
				Key: object.Key,
			})
		}
		
		if len(objectsToDelete) != 0 {
			_, err = c.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucketName),
				Delete: &types.Delete{
					Objects: objectsToDelete,
				},
			})
			if err != nil {
				log.LogErrorf("DeleteObjectsByPrefix delete objects failed, bucket: %s, keyPrefix: %s, objects: %v, err: %v",
					bucketName, keyPrefix, objectsToDelete, err)
			}
		}

		if !listResp.IsTruncated {
			break
		}

		maker = listResp.NextMarker
	}
	return
}
