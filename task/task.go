package task

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/housepower/clickhouse_sinker/input"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/output"
	"github.com/housepower/clickhouse_sinker/parser"

	"github.com/wswz/go_commons/log"
)

type TaskService struct {
	stopped    chan struct{}
	input      input.Input
	clickhouse *output.ClickHouse
	p          parser.Parser

	FlushInterval int
	BufferSize    int
	MinBufferSize int
}

func NewTaskService(input input.Input, clickhouse *output.ClickHouse, p parser.Parser) *TaskService {
	return &TaskService{
		stopped:    make(chan struct{}),
		input:      input,
		clickhouse: clickhouse,
		p:          p,
	}
}

func (service *TaskService) Init() error {
	err := service.input.Init()
	if err != nil {
		return err
	}
	return service.clickhouse.Init()
}

func (service *TaskService) Run() {
	if err := service.input.Start(); err != nil {
		panic(err)
	}

	log.Infof("TaskService %s TaskService has started", service.clickhouse.GetName())
	tick := time.NewTicker(time.Duration(service.FlushInterval) * time.Second)
	msgs := make([]model.Metric, 0, service.BufferSize)
FOR:
	for {
		select {
		case msg, more := <-service.input.Msgs():
			if !more {
				break FOR
			}
			msgs = append(msgs, service.parse(msg))
			if len(msgs) >= service.BufferSize {
				service.flush(msgs)
				msgs = msgs[:0]
				tick = time.NewTicker(time.Duration(service.FlushInterval) * time.Second)
			}
		case <-tick.C:
			log.Info(service.clickhouse.GetName() + " tick")
			if len(msgs) == 0 || len(msgs) < service.MinBufferSize {
				continue
			}
			service.flush(msgs)
			msgs = msgs[:0]
		}
	}
	service.flush(msgs)
	service.stopped <- struct{}{}
	return
}

func (service *TaskService) parse(data []byte) model.Metric {
	return service.p.Parse(data)
}
func (service *TaskService) flush(metrics []model.Metric) {
	log.Info("buf size:", len(metrics))
	service.clickhouse.LoopWrite(metrics)
}

func (service *TaskService) Stop() {
	log.Info("close TaskService size:")
	if err := service.input.Stop(); err != nil {
		panic(err)
	}
	<-service.stopped
	service.clickhouse.Close()
	log.Info("closed TaskService size:")
}

//获取goroutine的id
func GoID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}
