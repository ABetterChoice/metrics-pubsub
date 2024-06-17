package metrics

import (
	"context"
	"encoding/json"
	"runtime"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/abetterchoice/go-sdk/plugin/log"
	"github.com/abetterchoice/go-sdk/plugin/metrics"
	"github.com/abetterchoice/protoc_event_server"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

func (c *client) LogMonitorEventByProjectID(ctx context.Context, metadata *metrics.Metadata,
	eventGroup *protoc_event_server.MonitorEventGroup, projectID string) error {
	if isSync {
		return c.logMonitorEventByProjectID(ctx, metadata, eventGroup, projectID)
	}
	return asyncLogMonitorEvent(ctx, metadata, eventGroup, projectID)
}

func (c *client) LogMonitorEvent(ctx context.Context, metadata *metrics.Metadata,
	eventGroup *protoc_event_server.MonitorEventGroup) error {
	if isSync {
		return c.logMonitorEventByProjectID(ctx, metadata, eventGroup, "")
	}
	return asyncLogMonitorEvent(ctx, metadata, eventGroup, "")
}

type monitorEventInput struct {
	metadata   *metrics.Metadata
	eventGroup *protoc_event_server.MonitorEventGroup
	projectID  string
}

var (
	monitorEventChan      = make(chan *monitorEventInput, 1<<19)
	monitorEventInputPool = sync.Pool{
		New: func() any {
			return &monitorEventInput{}
		}}
)

func asyncLogMonitorEvent(ctx context.Context, metadata *metrics.Metadata,
	eventGroup *protoc_event_server.MonitorEventGroup, projectID string) error {
	data := monitorEventInputPool.Get().(*monitorEventInput)
	data.eventGroup = eventGroup
	data.projectID = projectID
	data.metadata = metadata
	select {
	case monitorEventChan <- data:
	default:
		log.Errorf("chan is busy")
	}
	return nil
}

func logMonitorEventLoop() {
	cacheData := make(map[string]*monitorEventInput)
	defer func() {
		// Prevent third-party monitoring reporting plugins from panicking
		recoverErr := recover()
		if recoverErr != nil {
			body := make([]byte, 1<<10)
			runtime.Stack(body, false)
			log.Errorf("recoverErr:%v\n%s", recoverErr, body)
			return
		}
	}()
	ticker := time.NewTicker(maxTimeInterval)
	defer ticker.Stop()
	for {
		select {
		case ei := <-monitorEventChan:
			key := ei.metadata.TableID
			if ei.projectID != "" {
				key = ei.projectID + ei.metadata.TableID
			}
			if cacheData[key] != nil {
				cacheData[key].eventGroup.Events = append(cacheData[key].eventGroup.Events, ei.eventGroup.Events...)
			} else {
				cacheData[key] = ei
			}
			if len(cacheData[key].eventGroup.Events) >= batchSize {
				err := Client.logMonitorEventByProjectID(context.TODO(), ei.metadata, cacheData[key].eventGroup, ei.projectID)
				if err != nil {
					log.Errorf("logMonitorEventByProjectID fail:%v", err)
				}
				delete(cacheData, key)
				monitorEventInputPool.Put(ei)
			}
		case <-ticker.C:
			for key, ei := range cacheData {
				if ei == nil || ei.eventGroup == nil || len(ei.eventGroup.Events) == 0 {
					continue
				}
				err := Client.logMonitorEventByProjectID(context.TODO(), ei.metadata, ei.eventGroup, ei.projectID)
				if err != nil {
					log.Errorf("logMonitorEventByProjectID fail:%v", err)
				}
				delete(cacheData, key)
				monitorEventInputPool.Put(ei)
			}
		}
	}
}

// logMonitorEventByProjectID Record surveillance exposure
func (c *client) logMonitorEventByProjectID(ctx context.Context, metadata *metrics.Metadata,
	eventGroup *protoc_event_server.MonitorEventGroup, projectID string) error {
	if metadata == nil || len(metadata.TableID) == 0 {
		log.Warnf("[pubsub]invalid tableID")
		return nil
	}
	if eventGroup == nil || len(eventGroup.Events) == 0 {
		return nil
	}
	pubsubClient, ok := c.cs[projectID]
	if !ok {
		pubsubClient = c.c
	}
	if pubsubClient == nil {
		return nil
	}
	topic := pubsubClient.Topic(metadata.TableID)
	topic.PublishSettings.Timeout = publishTimeout
	cfg, err := topicConfig(ctx, topic)
	if err != nil {
		return errors.Wrap(err, "topic config")
	}
	var data []byte
	if cfg.SchemaSettings != nil {
		switch cfg.SchemaSettings.Encoding {
		case pubsub.EncodingBinary:
			data, err = proto.Marshal(eventGroup)
			if err != nil {
				return errors.Wrap(err, "proto marshal")
			}
		case pubsub.EncodingJSON:
			data, err = json.Marshal(eventGroup)
			if err != nil {
				return errors.Wrap(err, "json marshal")
			}
		}
	} else {
		data, err = json.Marshal(eventGroup)
		if err != nil {
			return errors.Wrap(err, "json marshal")
		}
	}
	result := topic.Publish(ctx, &pubsub.Message{
		Attributes: map[string]string{},
		Data:       data,
	})
	_, err = result.Get(ctx)
	if err != nil {
		return err
	}
	return nil
}
