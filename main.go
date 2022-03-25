package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	config "github.com/aliyun-sls/skywalking-ingester/configure"
	"github.com/aliyun-sls/skywalking-ingester/converter"
	"github.com/aliyun-sls/skywalking-ingester/exporter"
	"github.com/aliyun-sls/skywalking-ingester/receiver"
)

func main() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	config := config.InitConfiguration()
	receiver, exporter, converter, e := initDataOperator(config)
	if e != nil {
		fmt.Println("Failed to init data operator", e)
		os.Exit(-1)
	}

	for {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
		default:
			orginData, e := receiver.ReceiveData()
			if e != nil {
				fmt.Printf("Failed to receiver data", e)
				continue
			}

			otData, t, e := converter.Convert(orginData)
			if e != nil {
				fmt.Println("Failed to convert data. ", e, "data: ", hex.EncodeToString(orginData.Data()))
			}

			err := exporter.Export(t, otData)
			if err != nil {
				fmt.Println("Failed to export data.", e)
			}
		}

	}

}

func initDataOperator(config config.Configuration) (r receiver.Receiver, e exporter.Exporter, c converter.Converter, err error) {
	r, err = receiver.NewReceiver(config)
	if err != nil {
		return
	}

	e, err = exporter.NewExporter(config)
	if err != nil {
		return
	}

	c = converter.NewConverter()
	return r, e, c, err
}
