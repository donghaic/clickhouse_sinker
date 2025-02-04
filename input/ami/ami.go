package ami

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

var GroupFormat = "xqu:%s:group"
var StreamFormat = "xqu:%s:%d"

func newClient(opt clientOptions) (*client, error) {
	// Fix for users, that forget set timeouts
	if opt.ropt.ReadTimeout < time.Second*30 {
		opt.ropt.ReadTimeout = time.Second * 30
	}
	if opt.ropt.WriteTimeout < time.Second*30 {
		opt.ropt.WriteTimeout = time.Second * 30
	}

	rDB := redis.NewClient(opt.ropt)

	c := &client{
		opt: opt,
		rDB: rDB,
	}

	err := c.init()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *client) init() error {
	group := fmt.Sprintf(GroupFormat, c.opt.name)
	for i := 0; i < int(c.opt.shardsCount); i++ {
		stream := fmt.Sprintf(StreamFormat, c.opt.name, i)
		err := c.createShard(stream, group)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *client) createShard(stream string, group string) error {
	xinfo := redis.NewCmd("XINFO", "STREAM", stream)

	err := c.rDB.Process(xinfo)
	// It is not an error, we only check stream existance
	if err != nil {
		xgroup := redis.NewCmd("XGROUP", "CREATE", stream, group, "$", "MKSTREAM")
		err := c.rDB.Process(xgroup)
		if err != nil {
			return err
		}
	}

	// Check after creation
	err = c.rDB.Process(xinfo)
	if err != nil {
		return err
	}

	return nil
}
