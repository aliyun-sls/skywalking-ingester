package converter

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/aliyun-sls/skywalking-ingester/modules"
	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/gogo/protobuf/proto"
	agentV3 "skywalking.apache.org/repo/goapi/collect/language/agent/v3"
)

type Converter interface {
	Convert(modules.OriginData) (*sls.LogGroup, error)
}

func NewConverter() Converter {
	return &convertImpl{}
}

type convertImpl struct {
}

func (c *convertImpl) Convert(data modules.OriginData) (*sls.LogGroup, error) {
	if data == nil {
		return nil, nil
	}

	switch data.(type) {
	case *modules.SegmentOriginData:
		if segment, err := c.convertSegmentObject(data.Data()); err != nil {
			return nil, err
		} else {
			return c.convertSegment(segment)
		}
	case *modules.MetricOriginData:
		return c.convertMetric(data.Data())
	case *modules.LogggingOriginData:
		return c.convertLogging(data.Data())
	default:
		return nil, nil
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

func (c *convertImpl) convertSegment(data *agentV3.SegmentObject) (slsData *sls.LogGroup, e error) {
	if data == nil || len(data.Spans) == 0 {
		return nil, nil
	}

	slsData = &sls.LogGroup{
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

	return slsData, nil
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
	contents = append(contents, appendAttributeToLogContent(StartTime, fmt.Sprint(span.StartTime*1000)))
	// end time
	contents = append(contents, appendAttributeToLogContent(EndTime, fmt.Sprint(span.EndTime*1000)))
	// duration
	contents = append(contents, appendAttributeToLogContent(Duration, fmt.Sprint(span.EndTime*1000-span.StartTime*1000)))
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
		Time:     proto.Uint32(uint32(span.StartTime)),
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

func (c *convertImpl) convertMetric(data []byte) (*sls.LogGroup, error) {
	return nil, nil
}

func (c *convertImpl) convertLogging(data []byte) (*sls.LogGroup, error) {
	return nil, nil
}
