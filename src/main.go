package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const totalParts = 10

type Config struct {
	FilePath   string
	BucketName string
	ObjectName string
	Endpoint   string
	AccessKey  string
	SecretKey  string
}

func uploadFileToCeph(config Config) {
	file, err := os.Open(config.FilePath)
	if err != nil {
		fmt.Println("打开文件时出错:", err)
		return
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println("获取文件信息时出错:", err)
		return
	}

	fileSize := fileInfo.Size()
	partSize := fileSize / int64(totalParts)

	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String(config.Endpoint),
		Credentials:      credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, ""),
		S3ForcePathStyle: aws.Bool(true), //客户端开启
	})
	if err != nil {
		fmt.Println("创建会话时出错:", err)
		return
	}

	svc := s3.New(sess)
	fmt.Println(svc)
	uploadID, err := createMultipartUpload(svc, config.BucketName, config.ObjectName)
	if err != nil {
		fmt.Println("创建分块上传时出错:", err)
		return
	}

	var completedParts []*s3.CompletedPart
	for i := 1; i <= totalParts; i++ {
		start := (int64(i) - 1) * partSize
		end := int64(i) * partSize
		if i == totalParts {
			end = fileSize
		}

		part, err := uploadPart(svc, file, config.BucketName, config.ObjectName, uploadID, i, start, end)
		if err != nil {
			fmt.Println("上传分块时出错:", err)
			abortMultipartUpload(svc, config.BucketName, config.ObjectName, uploadID)
			return
		}
		completedParts = append(completedParts, part)
	}

	err = completeMultipartUpload(svc, config.BucketName, config.ObjectName, uploadID, completedParts)
	if err != nil {
		fmt.Println("完成分块上传时出错:", err)
		return
	}

	fmt.Println("文件成功上传")
}

func createMultipartUpload(svc *s3.S3, bucketName, objectName string) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectName),
	}
	result, err := svc.CreateMultipartUpload(input)
	if err != nil {
		return "", err
	}
	return *result.UploadId, nil
}

func uploadPart(svc *s3.S3, file *os.File, bucketName, objectName, uploadID string, partNumber int, start, end int64) (*s3.CompletedPart, error) {
	partBuffer := make([]byte, end-start)
	_, err := file.ReadAt(partBuffer, start)
	if err != nil {
		return nil, err
	}

	input := &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(objectName),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int64(int64(partNumber)),
		Body:       bytes.NewReader(partBuffer),
	}
	result, err := svc.UploadPart(input)
	if err != nil {
		return nil, err
	}
	return &s3.CompletedPart{
		ETag:       result.ETag,
		PartNumber: aws.Int64(int64(partNumber)),
	}, nil
}

func abortMultipartUpload(svc *s3.S3, bucketName, objectName, uploadID string) {
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectName),
		UploadId: aws.String(uploadID),
	}
	_, err := svc.AbortMultipartUpload(input)
	if err != nil {
		fmt.Println("中止分块上传时出错:", err)
	}
}

func completeMultipartUpload(svc *s3.S3, bucketName, objectName, uploadID string, completedParts []*s3.CompletedPart) error {
	input := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucketName),
		Key:             aws.String(objectName),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: completedParts},
	}
	_, err := svc.CompleteMultipartUpload(input)
	return err
}

func main() {
	// 读取配置文件
	data, err := ioutil.ReadFile("config.json")
	if err != nil {
		fmt.Println("读取配置文件时出错:", err)
		return
	}

	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		fmt.Println("解析配置文件时出错:", err)
		return
	}

	uploadFileToCeph(config)
}
