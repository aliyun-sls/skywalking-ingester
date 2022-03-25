package exporter

import (
	"fmt"

	"github.com/aliyun-sls/skywalking-ingester/configure"
	"github.com/aliyun-sls/skywalking-ingester/modules"
	sls "github.com/aliyun/aliyun-log-go-sdk"
)

type Exporter interface {
	Export(modules.DataType, *sls.LogGroup) error
}

func NewExporter(config configure.Configuration) (Exporter, error) {
	return &exporterImpl{
		client: &sls.Client{
			Endpoint:        config.Endpoint(),
			AccessKeyID:     config.AccessKey(),
			AccessKeySecret: config.AccessSecurityKey(),
		},
		project:        config.Project(),
		logstore:       fmt.Sprintf("%s-traces", config.TraceInstance()),
		metricLogstore: fmt.Sprintf("%s-metrics", config.TraceInstance()),
	}, nil
}

type exporterImpl struct {
	client         *sls.Client
	project        string
	logstore       string
	metricLogstore string
}

func (e *exporterImpl) Export(t modules.DataType, data *sls.LogGroup) error {
	if data == nil {
		return nil
	}
	switch t {
	case modules.TRACE:
		return e.client.PutLogs(e.project, e.logstore, data)
	case modules.METRIC:
		return e.client.PutLogs(e.project, e.metricLogstore, data)
	}

	return nil
}
