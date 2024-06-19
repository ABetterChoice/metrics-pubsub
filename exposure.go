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

func (c *client) LogExposure(ctx context.Context, metadata *metrics.Metadata,
	exposureGroup *protoc_event_server.ExposureGroup) error {
	if isSync {
		return c.logExposureByProjectID(ctx, metadata, exposureGroup, "")
	}
	return asyncLogExposureByProjectID(ctx, metadata, exposureGroup, "")
}

func (c *client) LogExposureByProjectID(ctx context.Context, metadata *metrics.Metadata,
	exposureGroup *protoc_event_server.ExposureGroup, projectID string) error {
	if isSync {
		return c.logExposureByProjectID(ctx, metadata, exposureGroup, projectID)
	}
	return asyncLogExposureByProjectID(ctx, metadata, exposureGroup, projectID)
}

func asyncLogExposureByProjectID(ctx context.Context, metadata *metrics.Metadata,
	exposureGroup *protoc_event_server.ExposureGroup, projectID string) error {
	if metadata == nil || len(metadata.TableID) == 0 {
		log.Warnf("[pubsub]invalid tableID")
		return nil
	}
	if exposureGroup == nil || len(exposureGroup.Exposures) == 0 {
		return nil
	}
	data := exposureInputPool.Get().(*exposureInput)
	data.metadata = metadata
	data.exposureGroup = exposureGroup
	data.projectID = projectID
	select {
	case exposureChan <- data:
	default:
		log.Errorf("chan is busy")
	}
	return nil
}

func logExposureLoop() {
	cacheData := make(map[string]*exposureInput)
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
		case ei := <-exposureChan:
			key := ei.metadata.TableID
			if ei.projectID != "" {
				key = ei.projectID + ei.metadata.TableID
			}
			if cacheData[key] != nil {
				cacheData[key].exposureGroup.Exposures = append(cacheData[key].exposureGroup.Exposures,
					ei.exposureGroup.Exposures...)
			} else {
				cacheData[key] = ei
			}
			if len(cacheData[key].exposureGroup.Exposures) >= batchSize {
				err := Client.logExposureByProjectID(context.TODO(), ei.metadata, cacheData[key].exposureGroup, ei.projectID)
				if err != nil {
					log.Errorf("logExposure fail:%v", err)
				}
				delete(cacheData, key)
				exposureInputPool.Put(ei)
			}
		case <-ticker.C:
			for key, ei := range cacheData {
				if ei == nil || ei.exposureGroup == nil || len(ei.exposureGroup.Exposures) == 0 {
					continue
				}
				err := Client.logExposureByProjectID(context.TODO(), ei.metadata, ei.exposureGroup, ei.projectID)
				if err != nil {
					log.Errorf("logExposure fail:%v", err)
				}
				delete(cacheData, key)
				exposureInputPool.Put(ei)
			}
		}
	}
}

type exposureInput struct {
	metadata      *metrics.Metadata
	exposureGroup *protoc_event_server.ExposureGroup
	projectID     string // gcp project id
}

var (
	exposureChan      = make(chan *exposureInput, 1<<20)
	exposureInputPool = sync.Pool{
		New: func() any {
			return &exposureInput{}
		}}
)

// logExposureByProjectID 记录实验曝光 projectID 为 gcp 上的 projectID
func (c *client) logExposureByProjectID(ctx context.Context, metadata *metrics.Metadata,
	exposureGroup *protoc_event_server.ExposureGroup, projectID string) error {
	if metadata == nil || len(metadata.TableID) == 0 {
		log.Warnf("[pubsub]invalid tableID")
		return nil
	}
	if exposureGroup == nil || len(exposureGroup.Exposures) == 0 {
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
	var result *pubsub.PublishResult
	var data []byte
	if cfg.SchemaSettings != nil {
		switch cfg.SchemaSettings.Encoding {
		case pubsub.EncodingBinary:
			data, err = proto.Marshal(exposureGroup)
			if err != nil {
				return errors.Wrap(err, "proto marshal")
			}
		case pubsub.EncodingJSON:
			data, err = json.Marshal(exposureGroup)
			if err != nil {
				return errors.Wrap(err, "json marshal")
			}
		}
	} else {
		data, err = json.Marshal(exposureGroup)
		if err != nil {
			return errors.Wrap(err, "json marshal")
		}
	}
	result = topic.Publish(ctx, &pubsub.Message{
		// Attributes: map[string]string{
		//	"pid": exposureGroup.Exposures[i].ProjectId, // projectID 缩写 pid，用于染色，预留
		// },
		Data: data,
	})
	_, err = result.Get(ctx)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("[%s]publish", topic.String()))
	}
	return nil
}
