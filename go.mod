module github.com/aliyun-sls/skywalking-ingester

go 1.17

replace github.com/aliyun-sls/skywalking-ingester => ./

require (
	github.com/aliyun/aliyun-log-go-sdk v0.1.27
	github.com/confluentinc/confluent-kafka-go v1.7.0
	github.com/gogo/protobuf v1.3.1
	skywalking.apache.org/repo/goapi v0.0.0-20220322033350-0661327d31e3
	go.uber.org/zap v1.21.0
)

require (
	github.com/cenkalti/backoff v2.2.1+incompatible // indirect
	github.com/go-kit/kit v0.10.0 // indirect
	github.com/go-logfmt/logfmt v0.5.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.7 // indirect
	github.com/pierrec/lz4 v2.6.0+incompatible // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/stretchr/testify v1.7.1 // indirect
	golang.org/x/net v0.0.0-20211015210444-4f30a5c0130f // indirect
	golang.org/x/sys v0.0.0-20211019181941-9d821ace8654 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20210624195500-8bfb893ecb84 // indirect
	google.golang.org/grpc v1.40.0 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
)
