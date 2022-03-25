package modules

import "github.com/aliyun-sls/skywalking-ingester/configure"

type OriginData interface {
	Data() []byte
}

func NewOriginData(config configure.Configuration, topic string, data []byte) OriginData {
	switch topic {
	case config.SegmentTopic():
		return &SegmentOriginData{D: data}
	case config.MetricTopic():
		return &MetricOriginData{D: data}
	case config.LoggingTopic():
		return &LogggingOriginData{D: data}
	}
	return nil
}

type SegmentOriginData struct {
	D []byte
}

func (s *SegmentOriginData) Data() []byte {
	return s.D
}

type MetricOriginData struct {
	D []byte
}

func (s *MetricOriginData) Data() []byte {
	return s.D
}

type LogggingOriginData struct {
	D []byte
}

func (s *LogggingOriginData) Data() []byte {
	return s.D
}
