package configure

import (
	"flag"
	"fmt"
	"os"
)

type Configuration interface {
	Endpoint() string
	AccessKey() string
	AccessSecurityKey() string
	Project() string
	TraceInstance() string

	Topics() []string
	BootstrapServers() string
	MetricTopic() string
	SegmentTopic() string
	LoggingTopic() string
	GroupID() string
}

const (
	METRIC_TOPIC   = "skywalking-metrics"
	SEGMENTS_TOPIC = "skywalking-segments"
	LOGGING_TOPIC  = "skywalking-logging"
)

var (
	endpoint         string
	ak               string
	sk               string
	project          string
	traceInstance    string
	namespace        string
	bootstrapServers string
	groupID          string
)

func InitConfiguration() Configuration {
	flag.StringVar(&project, "project", os.Getenv("PROJECT"), "Project name")
	flag.StringVar(&endpoint, "endpoint", os.Getenv("ENDPOINT"), "endpoint")
	flag.StringVar(&ak, "access-key", os.Getenv("ACCESS_KEY"), "access key")
	flag.StringVar(&sk, "security-key", os.Getenv("SECURITY_KEY"), "security key")
	flag.StringVar(&traceInstance, "trace-instance", os.Getenv("TRACE_INSTANCE"), "trace instance")
	flag.StringVar(&namespace, "namespace", os.Getenv("NAMESPACE"), "namespace")
	flag.StringVar(&bootstrapServers, "bootstrap servers", os.Getenv("BOOTSTRAP_SERVERS"), "bootstrap servers")
	flag.StringVar(&groupID, "group", os.Getenv("GROUP"), "consumer group id")
	flag.Parse()

	if endpoint == "" || len(endpoint) == 0 {
		fmt.Println("Miss Parameter [endpoint]")
		os.Exit(-1)

	}

	if ak == "" || len(ak) == 0 {
		fmt.Println("Miss parameter [access key]")
		os.Exit(-1)

	}

	if sk == "" || len(sk) == 0 {
		fmt.Println("Miss parameter [access security key]")
		os.Exit(-1)
	}

	if project == "" || len(project) == 0 {
		fmt.Println("Miss parameter [project]")
		os.Exit(-1)
	}

	if traceInstance == "" {
		fmt.Println("Miss parameter [trace instace]")
		os.Exit(-1)
	}

	if bootstrapServers == "" {
		fmt.Println("Miss parameter [bootstrap servers]")
		os.Exit(-1)
	}

	if groupID == "" {
		groupID = "DEFAULT_SKYWALKING_INGESTER_GROUP"
	}

	return &configurationImpl{
		endpoint:         endpoint,
		ak:               ak,
		sk:               sk,
		project:          project,
		traceInstance:    traceInstance,
		namespace:        namespace,
		groupID:          groupID,
		bootstrapServers: bootstrapServers,
	}
}

type configurationImpl struct {
	endpoint         string
	ak               string
	sk               string
	project          string
	traceInstance    string
	namespace        string
	groupID          string
	bootstrapServers string
}

func (c *configurationImpl) BootstrapServers() string {
	return c.bootstrapServers
}
func (c *configurationImpl) Endpoint() string {
	return c.endpoint
}

func (c *configurationImpl) AccessKey() string {
	return c.ak
}

func (c *configurationImpl) AccessSecurityKey() string {
	return c.sk
}

func (c *configurationImpl) Project() string {
	return c.project
}

func (c *configurationImpl) Topics() []string {
	return []string{c.SegmentTopic(), c.MetricTopic(), c.LoggingTopic()}
}

func (c *configurationImpl) TraceInstance() string {
	return c.traceInstance
}

func (c *configurationImpl) MetricTopic() string {
	if c.namespace == "" {
		return METRIC_TOPIC
	}
	return fmt.Sprint("%s-%s", c.namespace, METRIC_TOPIC)
}

func (c *configurationImpl) SegmentTopic() string {
	if c.namespace == "" {
		return SEGMENTS_TOPIC
	}
	return fmt.Sprint("%s-%s", c.namespace, SEGMENTS_TOPIC)
}

func (c *configurationImpl) LoggingTopic() string {
	if c.namespace == "" {
		return LOGGING_TOPIC
	}
	return fmt.Sprint("%s-%s", c.namespace, LOGGING_TOPIC)
}

func (c *configurationImpl) GroupID() string {
	return c.groupID
}
