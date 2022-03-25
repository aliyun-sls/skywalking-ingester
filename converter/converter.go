package converter

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/aliyun-sls/skywalking-ingester/modules"
	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/golang/protobuf/proto"
	v3 "skywalking.apache.org/repo/goapi/collect/common/v3"
	agentV3 "skywalking.apache.org/repo/goapi/collect/language/agent/v3"
)

type Converter interface {
	Convert(modules.OriginData) (*sls.LogGroup, modules.DataType, error)
}

func NewConverter() Converter {
	return &convertImpl{}
}

type convertImpl struct {
}

func (c *convertImpl) Convert(data modules.OriginData) (*sls.LogGroup, modules.DataType, error) {
	if data == nil {
		return nil, modules.NOOP, nil
	}

	switch data.(type) {
	case *modules.SegmentOriginData:
		if segment, err := c.convertSegmentObject(data.Data()); err != nil {
			return nil, modules.TRACE, err
		} else {
			return c.convertSegment(segment)
		}
	case *modules.MetricOriginData:
		return c.convertMetric(data.Data())
	case *modules.LogggingOriginData:
		return c.convertLogging(data.Data())
	default:
		return nil, modules.NOOP, nil
	}
}

func (c *convertImpl) convertSegmentObject(data []byte) (segmentObject *agentV3.SegmentObject, e error) {
	defer func() {
		if r := recover(); r != nil {
			e = fmt.Errorf("Parse segement object failed")
		}
	}()

	segmentObject = &agentV3.SegmentObject{}
	if e = proto.Unmarshal(data, segmentObject); e != nil {
		return nil, e
	}
	return segmentObject, nil
}

func (c *convertImpl) convertSegment(data *agentV3.SegmentObject) (*sls.LogGroup, DataType, error) {
	if data == nil || len(data.Spans) == 0 {
		return nil, modules.TRACE, nil
	}

	slsData := &sls.LogGroup{
		Topic:  proto.String("0.0.0.0"),
		Source: proto.String(""),
	}

	for _, span := range data.Spans {
		if log, err := spanToLog(data, span); err == nil {
			slsData.Logs = append(slsData.Logs, log)
		} else {
			continue
		}
	}

	return slsData, modules.TRACE, nil
}

func spanToLog(data *agentV3.SegmentObject, span *agentV3.SpanObject) (*sls.Log, error) {
	contents := make([]*sls.LogContent, 0)

	// trace id
	contents = append(contents, appendAttributeToLogContent(TraceIDField, data.GetTraceId()))
	// span id
	contents = append(contents, appendAttributeToLogContent(SpanIDField, convertToOtelSpanID(data.GetTraceSegmentId(), span.GetSpanId())))
	// parent span id
	contents = append(contents, appendAttributeToLogContent(ParentSpanID, getParentSpanId(data, span)))
	// name
	contents = append(contents, appendAttributeToLogContent(OperationName, span.OperationName))
	// start time
	contents = append(contents, appendAttributeToLogContent(StartTime, strconv.FormatInt(span.StartTime*1000, 10)))
	// end time
	contents = append(contents, appendAttributeToLogContent(EndTime, strconv.FormatInt(span.EndTime*1000, 10)))
	// duration
	contents = append(contents, appendAttributeToLogContent(Duration, strconv.FormatInt(span.EndTime*1000-span.StartTime*1000, 10)))
	// service
	contents = append(contents, appendAttributeToLogContent(ServiceName, data.GetService()))
	// attribute
	contents = append(contents, appendAttributeToLogContent(Attribute, getAttribute(data, span)))
	// resource
	contents = append(contents, appendAttributeToLogContent(Resource, getResource(data)))
	// links
	contents = append(contents, appendAttributeToLogContent(Links, getLinks(span)))
	// logs
	contents = append(contents, appendAttributeToLogContent(Logs, getLogs(span)))
	// status message
	contents = append(contents, appendAttributeToLogContent(StatusMessageField, ""))
	// status code
	contents = append(contents, appendAttributeToLogContent(StatusCodeField, getStatusCode(span)))
	// span kind
	contents = append(contents, appendAttributeToLogContent(SpanKind, getSpanKind(span)))

	return &sls.Log{
		Time:     proto.Uint32(uint32(span.StartTime / int64(1000))),
		Contents: contents,
	}, nil
}

func getParentSpanId(data *agentV3.SegmentObject, span *agentV3.SpanObject) string {
	if span.GetParentSpanId() == -1 && len(span.Refs) == 0 {
		return ""
	} else if len(span.Refs) > 0 {
		ref := span.Refs[0]
		return convertToOtelSpanID(ref.ParentTraceSegmentId, ref.ParentSpanId)
	} else {
		return convertToOtelSpanID(data.GetTraceSegmentId(), span.GetParentSpanId())
	}
}

func getLinks(span *agentV3.SpanObject) string {
	if len(span.Refs) == 0 {
		return "[]"
	}

	links := make([]map[string]string, 0)

	for _, ref := range span.Refs {
		r := make(map[string]string)

		r["traceId"] = ref.TraceId
		r["spanID"] = convertToOtelSpanID(ref.ParentTraceSegmentId, ref.ParentSpanId)
		r["traceState"] = ""

		links = append(links, r)
	}

	if l, err := json.Marshal(links); err == nil {
		return string(l)
	} else {
		return "[]"
	}

}

func getAttribute(data *agentV3.SegmentObject, span *agentV3.SpanObject) string {
	if len(span.Tags) == 0 {
		return "{}"
	}

	attribute := make(map[string]string)

	for _, tag := range span.Tags {
		attribute[tag.Key] = tag.Value
	}

	if l, err := json.Marshal(attribute); err == nil {
		return string(l)
	} else {
		return "{}"
	}
}

func getLogs(span *agentV3.SpanObject) string {
	if len(span.Logs) == 0 {
		return "[]"
	}

	logs := make([]map[string]string, 0)

	for _, log := range span.Logs {
		e := make(map[string]string)
		e["time"] = strconv.FormatInt(log.Time, 10)
		for _, d := range log.Data {
			e[d.Key] = d.Value
		}

		logs = append(logs, e)
	}

	if l, err := json.Marshal(logs); err == nil {
		return string(l)
	} else {
		return fmt.Sprintln(logs)
	}
}

func getResource(data *agentV3.SegmentObject) string {
	resource := make(map[string]string)
	resource["service.name"] = data.GetService()
	resource["service.instance.id"] = data.GetServiceInstance()
	if d, err := json.Marshal(resource); err == nil {
		return string(d)
	} else {
		return ""
	}
}

func getStatusCode(span *agentV3.SpanObject) string {
	if span.GetIsError() {
		return "ERROR"
	} else {
		return "SUCCESS"
	}
}

func getSpanKind(span *agentV3.SpanObject) string {
	switch {
	case span.SpanLayer == agentV3.SpanLayer_MQ:
		if span.SpanType == agentV3.SpanType_Entry {
			return "consumer"
		} else {
			return "producer"
		}
	case span.GetSpanType() == agentV3.SpanType_Entry:
		return "server"
	case span.GetSpanType() == agentV3.SpanType_Exit:
		return "client"
	case span.GetSpanType() == agentV3.SpanType_Local:
		return "internal"
	default:
		return ""
	}
}

func convertToOtelSpanID(traceSegmentId string, spanID int32) string {
	return fmt.Sprintf("%s.%s", traceSegmentId, fmt.Sprint(spanID))
}

func appendAttributeToLogContent(k, v string) *sls.LogContent {
	return &sls.LogContent{
		Key:   proto.String(k),
		Value: proto.String(v),
	}
}

func (c *convertImpl) convertToMetric() {

}

func (c *convertImpl) convertMetric(data []byte) (l *sls.LogGroup, a modules.DataType, e error) {
	defer func() {
		if err := recover(); err != nil {
			e = fmt.Errorf("Failed to convert metric")
		}
	}()

	jvmMetric := &agentV3.JVMMetricCollection{}
	if e = proto.Unmarshal(data, jvmMetric); e != nil {
		return nil, modules.METRIC, e
	}

	if len(jvmMetric.Metrics) == 0 {
		return nil, modules.METRIC, nil
	}

	logs := make([]*sls.Log, 0)

	for _, metric := range jvmMetric.Metrics {
		c.convertCPU(jvmMetric, metric, metric.Cpu, logs)
		c.convertMemoryData(jvmMetric, metric, metric.Memory, logs)
		c.convertGCData(jvmMetric, metric, metric.Gc, logs)
		c.convertMemoryPool(jvmMetric, metric, metric.MemoryPool, logs)
		c.convertThread(jvmMetric, metric, logs)
	}
	return &sls.LogGroup{
		Source: proto.String("0.0.0.0"),
		Logs:   logs,
	}, modules.METRIC, nil

}

func (c *convertImpl) convertThread(jvmMetric *agentV3.JVMMetricCollection, metric *agentV3.JVMMetric, logs []*sls.Log) error {
	serviceName := newPair("service", jvmMetric.GetService())
	serviceInstance := newPair("serviceInstance", jvmMetric.GetServiceInstance())

	logs = append(logs, newMetric("skywalking_jvm_threads_live", metric.Time, strconv.FormatInt(metric.Thread.LiveCount, 10), serviceName, serviceInstance))
	logs = append(logs, newMetric("skywalking_jvm_threads_daemon", metric.Time, strconv.FormatInt(metric.Thread.DaemonCount, 10), serviceName, serviceInstance))
	logs = append(logs, newMetric("skywalking_jvm_threads_peak", metric.Time, strconv.FormatInt(metric.Thread.PeakCount, 10), serviceName, serviceInstance))
	return nil
}

func (c *convertImpl) convertCPU(jvmMetric *agentV3.JVMMetricCollection, metric *agentV3.JVMMetric, cpu *v3.CPU, logs []*sls.Log) error {
	serviceName := newPair("service", jvmMetric.GetService())
	serviceInstance := newPair("serviceInstance", jvmMetric.GetServiceInstance())
	logs = append(logs, newMetric("skywalking_jvm_cpu_usage", metric.Time, strconv.FormatFloat(cpu.UsagePercent, 'f', 6, 64), serviceName, serviceInstance))
	return nil
}

type Pair struct {
	key   string
	value string
}

func newPair(key string, value string) *Pair {
	return &Pair{
		key:   key,
		value: value,
	}
}

func newMetric(metric string, time int64, value string, labels ...*Pair) *sls.Log {
	strTime := strconv.FormatInt(time, 10)
	contents := make([]*sls.LogContent, 0)

	contents = append(contents, &sls.LogContent{
		Key:   proto.String("__name__"),
		Value: proto.String(metric),
	})

	contents = append(contents, &sls.LogContent{
		Key:   proto.String("__time_nano__"),
		Value: proto.String(strTime),
	})

	builder := strings.Builder{}
	for index, l := range labels {
		if index != 0 {
			builder.WriteString("|")
		}
		builder.WriteString(l.key)
		builder.WriteString("#$#")
		builder.WriteString(l.value)
	}

	contents = append(contents, &sls.LogContent{
		Key:   proto.String("__labels__"),
		Value: proto.String(builder.String()),
	})

	contents = append(contents, &sls.LogContent{
		Key:   proto.String("__value__"),
		Value: proto.String(value),
	})

	return &sls.Log{
		Time:     proto.Uint32(uint32(time / int64(1000))),
		Contents: contents,
	}
}

func (c *convertImpl) convertMemoryPool(jvmMetric *agentV3.JVMMetricCollection, metric *agentV3.JVMMetric, memoryPool []*agentV3.MemoryPool, logs []*sls.Log) (e error) {
	if len(memoryPool) == 0 {
		return nil
	}

	serviceName := newPair("service", jvmMetric.GetService())
	serviceInstance := newPair("serviceInstance", jvmMetric.GetServiceInstance())

	for _, i := range memoryPool {
		memoryType := newPair("type", i.GetType().String())
		logs = append(logs, newMetric("skywalking_jvm_memory_pool_committed", metric.GetTime(), strconv.FormatInt(i.Committed, 10), serviceName, serviceInstance, memoryType))
		logs = append(logs, newMetric("skywalking_jvm_memory_pool_init", metric.GetTime(), strconv.FormatInt(i.Init, 10), serviceName, serviceInstance, memoryType))
		logs = append(logs, newMetric("skywalking_jvm_memory_pool_max", metric.GetTime(), strconv.FormatInt(i.Max, 10), serviceName, serviceInstance, memoryType))
		logs = append(logs, newMetric("skywalking_jvm_memory_pool_used", metric.GetTime(), strconv.FormatInt(i.Used, 10), serviceName, serviceInstance, memoryType))
	}

	return nil
}

func (c *convertImpl) convertGCData(jvmMetric *agentV3.JVMMetricCollection, metric *agentV3.JVMMetric, gc []*agentV3.GC, logs []*sls.Log) (e error) {
	if len(gc) == 0 {
		return nil
	}

	serviceName := newPair("service", jvmMetric.GetService())
	serviceInstance := newPair("serviceInstance", jvmMetric.GetServiceInstance())

	for _, g := range gc {
		phrase := newPair("phrase", g.GetPhase().String())
		logs = append(logs, newMetric("skywalking_jvm_gc_time", metric.GetTime(), strconv.FormatInt(g.GetTime(), 10), phrase, serviceName, serviceInstance))
		logs = append(logs, newMetric("skywalking_jvm_gc_count", metric.GetTime(), strconv.FormatInt(g.GetCount(), 10), phrase, serviceName, serviceInstance))
	}

	return nil
}

func (c *convertImpl) convertMemoryData(jvmMetric *agentV3.JVMMetricCollection, metric *agentV3.JVMMetric, memory []*agentV3.Memory, logs []*sls.Log) (e error) {
	if len(memory) == 0 {
		return nil
	}

	serviceName := newPair("service", jvmMetric.GetService())
	serviceInstance := newPair("serviceInstance", jvmMetric.GetServiceInstance())

	for _, m := range memory {
		memType := "nonheap"

		if m.IsHeap {
			memType = "heap"
		}
		memTypeLabel := newPair("type", memType)
		logs = append(logs, newMetric("skywalking_jvm_memory_committed", metric.GetTime(), strconv.FormatInt(m.Committed, 10), serviceName, serviceInstance, memTypeLabel))
		logs = append(logs, newMetric("skywalking_jvm_memory_init", metric.GetTime(), strconv.FormatInt(m.Init, 10), serviceName, serviceInstance, memTypeLabel))
		logs = append(logs, newMetric("skywalking_jvm_memory_max", metric.GetTime(), strconv.FormatInt(m.Max, 10), serviceName, serviceInstance, memTypeLabel))
		logs = append(logs, newMetric("skywalking_jvm_memory_used", metric.GetTime(), strconv.FormatInt(m.Used, 10), serviceName, serviceInstance, memTypeLabel))
	}

	return e
}

func (c *convertImpl) convertLogging(data []byte) (*sls.LogGroup, modules.DataType, error) {
	return nil, modules.LOGGING, nil
}
