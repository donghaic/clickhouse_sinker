package input

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis"
	"github.com/housepower/clickhouse_sinker/input/ami"
	"github.com/wswz/go_commons/log"
	"sync"
	"time"
)

type errorLogger struct{}

func (l *errorLogger) AmiError(err error) {
	log.Error("Got error from Ami:", err.Error())
}

type Redis struct {
	consumer *ami.Consumer
	stopped  chan struct{}
	msgs     chan ([]byte)

	Topic         string
	BufferSize    int
	Concurrency   int8
	PrefetchCount int64
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

	log.Info("Name----", k.Name)
	log.Info("Concurrency----", k.Concurrency)
	log.Info("Addr----", k.Addr)
	log.Info("Password----", k.Password)
	log.Info("PoolSize----", k.PoolSize)

	consumer, err := ami.NewConsumer(
		ami.ConsumerOptions{
			Consumer:          k.Name,
			ErrorNotifier:     &errorLogger{},
			Name:              k.Topic,
			PendingBufferSize: 10000000,
			PipeBufferSize:    50000,
			PipePeriod:        time.Microsecond * 1000,
			PrefetchCount:     k.PrefetchCount,
			ShardsCount:       k.Concurrency,
		},
		&redis.Options{
			Addr:         k.Addr,
			ReadTimeout:  time.Second * 60,
			WriteTimeout: time.Second * 60,
			Password:     k.Password,
			PoolSize:     k.PoolSize,
		},
	)

	if err != nil {
		log.Info("init redis error. %s", err.Error())
		return err
	}

	k.consumer = consumer
	k.context, k.cancel = context.WithCancel(context.Background())
	return nil
}

func (k *Redis) Msgs() chan []byte {
	return k.msgs
}
func (k *Redis) Start() error {

	log.Info(" Register topic = ", k.Topic)
	c := k.consumer.Start()
	go func() {
		for {
			m, more := <-c
			if !more {
				time.Sleep(time.Second * 2)
				continue
			}
			k.msgs <- []byte(m.Body)
			k.consumer.Ack(m)
		}
	}()
	return nil
}

func (k *Redis) Stop() error {
	k.cancel()
	k.wg.Wait()

	k.consumer.Close()
	close(k.msgs)
	return nil
}
