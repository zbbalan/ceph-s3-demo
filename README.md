# ceph-s3-demo
## 1.执行命令下载依赖

```shell
go mod tidy
```

## 2.打包成二进制可执行文件

```sh
   go build main.go
```

## 3.配置文件在cong.json里

```shell
FilePath: 上传文件路径
BucketName：存储桶名称
ObjectName：对象名称
Endpoint：S3地址
AccessKey：秘钥
SecretKey：秘钥
```

