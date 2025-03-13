package infra

import (
	"context"

	"github.com/HavvokLab/true-solar/config"
	"github.com/go-redis/redis/v8"
)

func NewRedis() (*redis.Client, error) {
	conf := config.GetConfig().Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     conf.Host + ":" + conf.Port,
		Username: conf.Username,
		Password: conf.Password,
		DB:       conf.DB,
	})

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return rdb, nil
}
