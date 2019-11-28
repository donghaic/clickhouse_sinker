package creator

import (
	"encoding/json"
	"github.com/housepower/clickhouse_sinker/input"
	"github.com/housepower/clickhouse_sinker/output"
	"github.com/housepower/clickhouse_sinker/parser"
	"github.com/housepower/clickhouse_sinker/task"
	"github.com/housepower/clickhouse_sinker/util"
	"github.com/wswz/go_commons/log"
)

// GenTasks generate the tasks via config
func (config *Config) GenTasks() []*task.TaskService {
	res := make([]*task.TaskService, 0, len(config.Tasks))
	for _, taskConfig := range config.Tasks {
		input := config.GenInput(taskConfig)
		ck := config.GenOutput(taskConfig)
		p := parser.NewParser(taskConfig.Parser, taskConfig.CsvFormat, taskConfig.Delimiter)

		taskImpl := task.NewTaskService(input, ck, p)

		util.IngestConfig(taskConfig, taskImpl)

		if taskImpl.FlushInterval == 0 {
			taskImpl.FlushInterval = config.Common.FlushInterval
		}

		if taskImpl.BufferSize == 0 {
			taskImpl.BufferSize = config.Common.BufferSize
		}

		if taskImpl.MinBufferSize == 0 {
			taskImpl.MinBufferSize = config.Common.MinBufferSize
		}

		res = append(res, taskImpl)
	}
	return res
}

// GenInput generate the input via config
 func (config *Config) GenInput(taskCfg *Task) input.Input {
	log.Info("gen input ..")
	if config.Kafka != nil && len(config.Kafka) > 0 {
		log.Info("gen Kafka input ..")
		kfkCfg := config.Kafka[taskCfg.Kafka]
		inputImpl := input.NewKafka()
		util.IngestConfig(kfkCfg, inputImpl)
		util.IngestConfig(taskCfg, inputImpl)

		return inputImpl
	} else {
		log.Info("gen Redis input ..", taskCfg.Redis)
		redisConfig := config.Redis[taskCfg.Redis]

		log.Info("addr config == ", redisConfig.Addr)
		log.Info("PoolSize config == ", redisConfig.PoolSize)
		inputImpl := input.NewRedis()
		log.Info("redis len =", len(config.Redis))
		bytes, e := json.Marshal(config.Redis)
		if e != nil {
			panic(e.Error())
		}
		log.Info("Redis SetConfig == ", string(bytes))
		util.IngestConfig(taskCfg, inputImpl)
		util.IngestConfig(redisConfig, inputImpl)
		return inputImpl
	}

}
// GenOutput generate the output via config
func (config *Config) GenOutput(taskCfg *Task) *output.ClickHouse {
	ckCfg := config.Clickhouse[taskCfg.Clickhouse]

	outputImpl := output.NewClickHouse()

	util.IngestConfig(ckCfg, outputImpl)
	util.IngestConfig(taskCfg, outputImpl)
	util.IngestConfig(config.Common, outputImpl)
	return outputImpl
}
