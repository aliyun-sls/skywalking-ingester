package exporter

import (
	"fmt"

	"github.com/aliyun-sls/skywalking-ingester/configure"
	sls "github.com/aliyun/aliyun-log-go-sdk"
)

type Exporter interface {
	Export(*sls.LogGroup) error
}

func NewExporter(config configure.Configuration) (Exporter, error) {
	return &exporterImpl{
		client: &sls.Client{
			Endpoint:        config.Endpoint(),
			AccessKeyID:     config.AccessKey(),
			AccessKeySecret: config.AccessSecurityKey(),
		},
		project:  config.Project(),
		logstore: fmt.Sprintf("%s-traces", config.TraceInstance()),
	}, nil
}

type exporterImpl struct {
	client   *sls.Client
	project  string
	logstore string
}

func (e *exporterImpl) Export(data *sls.LogGroup) error {
	if data == nil {
		return nil
	}

	return e.client.PutLogs(e.project, e.logstore, data)
}
