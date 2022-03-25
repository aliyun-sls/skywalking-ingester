package modules

import "github.com/aliyun-sls/skywalking-ingester/configure"

type OriginData interface {
	Data() []byte
}

func NewOriginData(config configure.Configuration, topic string, data []byte) OriginData {
	switch topic {
	case config.SegmentTopic():
		return &SegmentOriginData{d: data}
	case config.MetricTopic():
		return &MetricOriginData{d: data}
	case config.LoggingTopic():
		return &LogggingOriginData{d: data}
	}
	return nil
}

type SegmentOriginData struct {
	d []byte
}

func (s *SegmentOriginData) Data() []byte {
	return s.d
}

type MetricOriginData struct {
	d []byte
}

func (s *MetricOriginData) Data() []byte {
	return s.d
}

type LogggingOriginData struct {
	d []byte
}

func (s *LogggingOriginData) Data() []byte {
	return s.d
}
