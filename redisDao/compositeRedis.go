package redisDao

import "github.com/go-redis/redis/v8"

type CompositeRedis struct {
	Redis redis.Client
	DB    interface{}
}
