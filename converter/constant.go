package converter

const (
	// ParentService The log item key of parent service name
	ParentService = "parent_service"
	// ChildService the field name of child service
	ChildService = "child_service"
	// ServiceName the field name of service
	ServiceName = "service"
	// OperationName the field name of operation name
	OperationName = "name"
	// SpanKind  the field name of span kind
	SpanKind = "kind"
	//
	Host = "host"
	// TraceID the field name of trace id
	TraceID = "traceID"
	// TraceIDField
	TraceIDField = "traceid"
	// SpanID the field name of span id
	SpanID = "spanID"
	// SpanIDField
	SpanIDField = "spanID"
	// ParentSpanID the field name of parent span id
	ParentSpanID = "parentSpanID"
	// StartTime the field name of start time
	StartTime = "start"
	// Duration the field name of duration
	Duration = "duration"
	// Attribute the field name of span tags
	Attribute = "attribute"
	// Resource the field name of span process tag
	Resource = "resource"
	// Logs the field name of span log
	Logs = "logs"
	// Links the field name of span reference
	Links = "links"
	// StatusMessage the field name of warning message of span
	StatusMessage = "statusMessage"
	//StatusMessageField
	StatusMessageField = "statusmessage"
	// Flags the field name of flags
	Flags = "flags"
	// EndTime the field name of end time
	EndTime = "end"
	// StatusCode the field name of status code
	StatusCode = "statusCode"
	// StatusCodeField
	StatusCodeField = "statuscode"
)

const (
	AttributeRefType                  = "refType"
	AttributeParentService            = "parent.service"
	AttributeParentInstance           = "parent.service.instance"
	AttributeParentEndpoint           = "parent.endpoint"
	AttributeNetworkAddressUsedAtPeer = "network.AddressUsedAtPeer"
)

var otSpanTagsMapping = map[string]string{
	"url":         "",
	"status_code": "http.status_code",
	"db.type":     "db.system",
	"db.instance": "",
	"mq.broker":   "",
}
