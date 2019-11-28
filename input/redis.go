package input

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/robinjoseph08/redisqueue"
	"github.com/wswz/go_commons/log"
	"sync"
	"time"
)

type Redis struct {
	consumer *redisqueue.Consumer
	stopped  chan struct{}
	msgs     chan ([]byte)

	Topic         string
	BufferSize    int
	Concurrency   int
	ConsumerGroup string
	Name          string
	// redis
	Addr               string
	Password           string
	PoolSize           int
	MinIdleConns       int
	MaxConnAge         time.Duration
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration

	context context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func NewRedis() *Redis {
	return &Redis{}
}

func (k *Redis) SetConfig(config interface{}) {
	bytes, _ := json.Marshal(config)
	log.Info("SetConfig == ", string(bytes))
}

func (k *Redis) Init() error {
	log.Info("init redis")
	k.msgs = make(chan []byte, 300000)
	k.stopped = make(chan struct{})

	consumerOptions := &redisqueue.ConsumerOptions{
		GroupName:         k.ConsumerGroup,
		Name:              k.Name,
		VisibilityTimeout: 60 * time.Second,
		BlockingTimeout:   5 * time.Second,
		ReclaimInterval:   1 * time.Second,
		BufferSize:        k.BufferSize,
		Concurrency:       k.Concurrency,
		RedisOptions: &redis.Options{
			Addr:       k.Addr,
			Password:   k.Password,
			MaxRetries: 3,
			PoolSize:   k.PoolSize,
		},
	}

	log.Info("----", consumerOptions.GroupName)
	log.Info("----", consumerOptions.Name)
	log.Info("Addr----", consumerOptions.RedisOptions.Addr)
	log.Info("PoolSize----", consumerOptions.RedisOptions.PoolSize)

	c, err := redisqueue.NewConsumerWithOptions(consumerOptions)

	//c, err := redisqueue.NewConsumerWithOptions(&redisqueue.ConsumerOptions{
	//	GroupName:         "cg_2",
	//	Name:              "res",
	//	VisibilityTimeout: 60 * time.Second,
	//	BlockingTimeout:   5 * time.Second,
	//	ReclaimInterval:   1 * time.Second,
	//	BufferSize:        100,
	//	Concurrency:       1,
	//})
	if err != nil {
		log.Info("init redis error. %s", err.Error())
		return err
	}

	k.consumer = c
	k.context, k.cancel = context.WithCancel(context.Background())
	return nil
}

func (k *Redis) Msgs() chan []byte {
	return k.msgs
}
func (k *Redis) Start() error {

	log.Info(" Register %s", k.Topic)
	k.consumer.Register(k.Topic, k.process)
	go func() {
		for err := range k.consumer.Errors {
			// handle errors accordingly
			fmt.Printf("err: %+v\n", err)
		}
	}()

	log.Info("consumer starting")
	go k.consumer.Run()
	log.Info("consumer Run")
	return nil
}

func (k *Redis) Stop() error {
	k.cancel()
	k.wg.Wait()

	k.consumer.Shutdown()
	close(k.msgs)
	return nil
}

func (k *Redis) process(msg *redisqueue.Message) error {
	v, ok := msg.Values["m"]
	if ok {
		body := v.(string)
		k.msgs <- []byte(body)
	}
	return nil
}
