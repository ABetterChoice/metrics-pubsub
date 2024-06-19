package metrics

import (
	"context"
	"encoding/json"
	"fmt"
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

func (c *client) LogEventByProjectID(ctx context.Context, metadata *metrics.Metadata,
	eventGroup *protoc_event_server.EventGroup, projectID string) error {
	if isSync {
		return c.logEventByProjectID(ctx, metadata, eventGroup, projectID)
	}
	return asyncLogCustomizationEvent(ctx, metadata, eventGroup, projectID)
}

func (c *client) LogEvent(ctx context.Context, metadata *metrics.Metadata,
	eventGroup *protoc_event_server.EventGroup) error {
	if isSync {
		return c.logEventByProjectID(ctx, metadata, eventGroup, "")
	}
	return asyncLogCustomizationEvent(ctx, metadata, eventGroup, "")
}

type customizationEventInput struct {
	metadata   *metrics.Metadata
	eventGroup *protoc_event_server.EventGroup
	projectID  string
}

var (
	customizationEventChan      = make(chan *customizationEventInput, 1<<19)
	customizationEventInputPool = sync.Pool{
		New: func() any {
			return &customizationEventInput{}
		}}
)

func asyncLogCustomizationEvent(ctx context.Context, metadata *metrics.Metadata,
	eventGroup *protoc_event_server.EventGroup, projectID string) error {
	data := customizationEventInputPool.Get().(*customizationEventInput)
	data.eventGroup = eventGroup
	data.projectID = projectID
	data.metadata = metadata
	select {
	case customizationEventChan <- data:
	default:
		log.Errorf("chan is busy")
	}
	return nil
}

func logCustomizationEventLoop() {
	cacheData := make(map[string]*customizationEventInput)
	defer func() {
		recoverErr := recover() // 防止第三方实现的监控上报插件 panic
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
		case ei := <-customizationEventChan:
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
				err := Client.logEventByProjectID(context.TODO(), ei.metadata, cacheData[key].eventGroup, ei.projectID)
				if err != nil {
					log.Errorf("logMonitorEventByProjectID fail:%v", err)
				}
				delete(cacheData, key)
				customizationEventInputPool.Put(ei)
			}
		case <-ticker.C:
			for key, ei := range cacheData {
				if ei == nil || ei.eventGroup == nil || len(ei.eventGroup.Events) == 0 {
					continue
				}
				err := Client.logEventByProjectID(context.TODO(), ei.metadata, ei.eventGroup, ei.projectID)
				if err != nil {
					log.Errorf("[ticker]logMonitorEventByProjectID fail:%v", err)
				}
				delete(cacheData, key)
				customizationEventInputPool.Put(ei)
			}
		}
	}
}

// logEventByProjectID 记录事件曝光
func (c *client) logEventByProjectID(ctx context.Context, metadata *metrics.Metadata,
	eventGroup *protoc_event_server.EventGroup, projectID string) error {
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
		return errors.Wrap(err, fmt.Sprintf("[%s]topic config", topic.String()))
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
		return errors.Wrap(err, fmt.Sprintf("[%s]publish", topic.String()))
	}
	return nil
}
