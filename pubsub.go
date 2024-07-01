// Package metrics TODO
package metrics

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/abetterchoice/go-sdk/plugin/log"
	"github.com/abetterchoice/go-sdk/plugin/metrics"
	"github.com/abetterchoice/protoc_cache_server"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/option"
)

// PluginName pubsub Monitoring reporting plug-in name
const PluginName = "pubsub"

const (
	// ProjectIDKey TODO
	ProjectIDKey = "projectID" // project ID
	// TopicNameKey TODO
	TopicNameKey = "topicName" // topic topic name
	// CredentialsJSON TODO
	CredentialsJSON = "credentialsJSON" // Authentication token, optional. If not, GCP default key management will be used.
)

// Name Plugin Name
func (c *client) Name() string {
	return PluginName
}

// InitMultiConfig Initializing pubsub
func (c *client) InitMultiConfig(ctx context.Context, configs ...*protoc_cache_server.MetricsInitConfig) (err error) {
	if len(configs) == 0 {
		return errors.Errorf("configs is required")
	}
	defer func() {
		if err != nil {
			Release()
		}
	}()
	for _, config := range configs {
		if config == nil {
			return fmt.Errorf("[pubsub]config is required")
		}
		if len(config.Kv) == 0 {
			return fmt.Errorf("[pubsub]invalid config")
		}
		projectID, ok := config.Kv[ProjectIDKey]
		if !ok || len(projectID) == 0 {
			return fmt.Errorf("[pubsub]invalid projectID")
		}
	}
	initOnce.Do(func() {
		asyncLog()
		for _, config := range configs {
			projectID := config.Kv[ProjectIDKey]
			var opts []option.ClientOption
			credentialsJSON, ok := config.Kv[CredentialsJSON]
			if ok && len(credentialsJSON) > 0 {
				var keyJSON []byte
				keyJSON, err = base64.StdEncoding.DecodeString(credentialsJSON)
				if err != nil {
					log.Errorf("[pubsub]decodeString fail:%v", err)
					return
				}
				var conf *jwt.Config
				conf, err = google.JWTConfigFromJSON(keyJSON, pubsub.ScopePubSub, pubsub.ScopeCloudPlatform)
				if err != nil {
					log.Errorf("[pubsub]jwtConfigFromJSON fail:%v", err)
					return
				}
				opts = append(opts, option.WithTokenSource(conf.TokenSource(ctx)))
			}
			var pubsubClient *pubsub.Client
			pubsubClient, err = pubsub.NewClient(ctx, projectID, opts...)
			if err != nil {
				return
			}
			c.c = pubsubClient
			if c.cs == nil {
				c.cs = make(map[string]*pubsub.Client)
			}
			c.cs[projectID] = pubsubClient
		}
	})
	return err
}

// Init Initializing pubsub
func (c *client) Init(ctx context.Context, config *protoc_cache_server.MetricsInitConfig) (err error) {
	if config == nil {
		return fmt.Errorf("[pubsub]config is required")
	}
	if len(config.Kv) == 0 {
		return fmt.Errorf("[pubsub]invalid config")
	}
	projectID, ok := config.Kv[ProjectIDKey]
	if !ok || len(projectID) == 0 {
		return fmt.Errorf("[pubsub]invalid projectID")
	}
	// topicName, ok := config.Kv[TopicNameKey]
	// if !ok || len(topicName) == 0 {
	//	return fmt.Errorf("invalid topicName")
	// }
	defer func() {
		if err != nil {
			Release()
		}
	}()
	initOnce.Do(func() {
		asyncLog()
		var opts []option.ClientOption
		credentialsJSON, ok := config.Kv[CredentialsJSON]
		if ok && len(credentialsJSON) > 0 {
			var keyJSON []byte
			keyJSON, err = base64.StdEncoding.DecodeString(credentialsJSON)
			if err != nil {
				log.Errorf("[pubsub]decodeString fail:%v", err)
				return
			}
			var conf *jwt.Config
			conf, err = google.JWTConfigFromJSON(keyJSON, pubsub.ScopePubSub, pubsub.ScopeCloudPlatform)
			if err != nil {
				log.Errorf("[pubsub]jwtConfigFromJSON fail:%v", err)
				return
			}
			opts = append(opts, option.WithTokenSource(conf.TokenSource(ctx)))
		}
		var pubsubClient *pubsub.Client
		pubsubClient, err = pubsub.NewClient(ctx, projectID, opts...)
		if err != nil {
			return
		}
		c.c = pubsubClient
	})
	return err
}

// Release Release resources
func Release() error {
	initOnce = sync.Once{}
	if Client == nil || Client.c == nil {
		return nil
	}
	return Client.c.Close()
}

var (
	// Client pubsub client wrapper
	Client   = &client{}
	initOnce sync.Once
)

var isSync = false

// SetIsSync TODO
func SetIsSync(b bool) {
	isSync = b
}

func init() {
	metrics.RegisterClient(Client)
}

// client pubsub Report implementation
type client struct {
	c  *pubsub.Client
	cs map[string]*pubsub.Client
}

const defaultMaxParallelism = 4

func maxParallelism() int {
	maxProcs := runtime.GOMAXPROCS(0)
	if maxProcs > defaultMaxParallelism {
		return maxProcs
	}
	return defaultMaxParallelism
}

func asyncLog() {
	for i := 0; i < maxParallelism(); i++ {
		go logExposureLoop()
		go logMonitorEventLoop()
		go logCustomizationEventLoop()
	}
}

const (
	batchSize       = 2000
	maxTimeInterval = 3 * time.Second
)

// SendData General reporting, reserved, it is recommended to use logEvent TODO to go to logEvent
func (c *client) SendData(ctx context.Context, metadata *metrics.Metadata, data [][]string) error {
	if metadata == nil || len(metadata.TableID) == 0 {
		log.Warnf("[pubsub]invalid tableID")
		return nil
	}
	if len(data) == 0 {
		return nil
	}
	var wg sync.WaitGroup
	var totalErrors uint64
	for i := range data {
		if len(data[i]) == 0 {
			continue
		}
		body, err := json.Marshal(data[i])
		if err != nil {
			atomic.AddUint64(&totalErrors, 1)
			continue
		}
		if c.c == nil {
			return nil
		}
		result := c.c.Topic(metadata.TableID).Publish(ctx, &pubsub.Message{
			Attributes: map[string]string{
				"ct": "json", // contentType is json, default is pb
			},
			Data: body,
		})
		wg.Add(1)
		go func(i int, res *pubsub.PublishResult) {
			defer wg.Done()
			// The Get method blocks until a server-generated ID or
			// an error is returned for the published message.
			id, err := res.Get(ctx)
			if err != nil {
				// Error handling code can be added here.
				log.Errorf("[pubsub id=%v]Failed to publish: %v", err, id)
				atomic.AddUint64(&totalErrors, 1)
				return
			}
		}(i, result)
	}
	wg.Wait()
	if totalErrors > 0 {
		return fmt.Errorf("%d of %d messages did not publish successfully", totalErrors, len(data))
	}
	return nil
}

var topicConfigCache sync.Map
var rwMutex sync.RWMutex

var publishTimeout = 1 * time.Second

// SetPublishTimeout TODO
func SetPublishTimeout(t time.Duration) {
	if t < 10*time.Millisecond {
		t = 10 * time.Millisecond
	}
	publishTimeout = t
}

func topicConfig(ctx context.Context, topic *pubsub.Topic) (*pubsub.TopicConfig, error) {
	localCfg, ok := topicConfigCache.Load(topic.String())
	if ok {
		cfg, isKind := localCfg.(*pubsub.TopicConfig)
		if isKind {
			return cfg, nil
		}
	}
	rwMutex.Lock()
	defer rwMutex.Unlock()
	localCfg, ok = topicConfigCache.Load(topic.String())
	if ok {
		cfg, isKind := localCfg.(*pubsub.TopicConfig)
		if isKind {
			return cfg, nil
		}
	}
	defer func(start time.Time) {
		log.Infof("topicConfig cost=%s", time.Since(start))
	}(time.Now())
	cfg, err := topic.Config(ctx)
	if err != nil {
		return &cfg, err
	}
	topicConfigCache.Store(topic.String(), &cfg)
	return &cfg, err
}
