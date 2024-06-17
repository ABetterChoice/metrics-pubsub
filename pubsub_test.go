// Package metrics ...
package metrics

import (
	"context"
	"testing"

	"github.com/abetterchoice/go-sdk/plugin/metrics"
	"github.com/abetterchoice/protoc_cache_server"
	"github.com/abetterchoice/protoc_event_server"
)

func TestPubsubBase(t *testing.T) {
	err := Client.Init(context.TODO(), &protoc_cache_server.MetricsInitConfig{
		Region: "cn",
		Addr:   "",
		Kv: map[string]string{
			ProjectIDKey:    "abetterchoice",
			CredentialsJSON: "",
		},
	})
	if err != nil {
		t.Fatalf("init fail:%v", err)
		return
	}
	isSync = true
	err = Client.LogMonitorEvent(context.TODO(), &metrics.Metadata{
		MetricsPluginName: "",
		TableName:         "",
		TableID:           "",
		Token:             "",
		SamplingInterval:  0,
	}, &protoc_event_server.MonitorEventGroup{Events: []*protoc_event_server.MonitorEvent{
		{
			EventName: "test",
		},
	}})
	if err != nil {
		t.Fatalf("logMonitorEvent fail:%v", err)
		return
	}
}
