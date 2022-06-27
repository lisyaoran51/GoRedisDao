package redisDao

import (
	"context"
	"reflect"

	"github.com/go-redis/redis/v8"
	"github.com/lisyaoran51/GoRedisDao"
	"github.com/lisyaoran51/GoRedisDao/candleDao"
	"github.com/lisyaoran51/GoRedisDao/model"
)

type REDIS_STRATEGY int

const (
	REDIS_STRATEGY_NONE REDIS_STRATEGY = iota
	REDIS_STRATEGY_REDIS_ONLY
	REDIS_STRATEGY_CACHE_ASIDE
	REDIS_STRATEGY_DOUBLE_DELETE
	REDIS_STRATEGY_RW_THROUGH
	REDIS_STRATEGY_WRITE_BEHIND
)

type RedisDao interface {
	GoRedisDao.Dao
	GetKeys(ctx context.Context, rdb *redis.Client) []string
	CleanKeys(ctx context.Context, rdb *redis.Client, keys []string) error
	Clean(ctx context.Context, rdb *redis.Client, cleanFunc func([]string) []string) error
	DeepNew(ctx context.Context, dataSource interface{}, model *model.CandleModel) (result interface{}, err error)
	DeepGet(ctx context.Context, dataSource interface{}, query candleDao.CandleQueryModel) (result *model.CandleModel, err error)
	DeepGets(ctx context.Context, dataSource interface{}, query candleDao.CandleQueryModel) (result []*model.CandleModel, err error)
	DeepModify(ctx context.Context, dataSource interface{}, model *model.CandleModel, fields []candleDao.CandleUpdateField) (err error)
	DeepDelete(ctx context.Context, dataSource interface{}, query candleDao.CandleQueryModel) (err error)
}

type RedisDaoImpl struct {
	GoRedisDao.Dao
	RedisDao
}

func (r *RedisDaoImpl) GetSourceType() string {
	source := &CompositeRedis{}
	return reflect.TypeOf(source).String()
}

func (r *RedisDaoImpl) Transaction(ctx *context.Context, dataSource interface{}, txFunc func(dataSource interface{}) error) error {
	if rdb, ok := dataSource.(*CompositeRedis); ok {
		return r.transaction(ctx, rdb, txFunc)
	}
	return GoRedisDao.ErrInternal
}

func (r *RedisDaoImpl) transaction(ctx *context.Context, rdb *CompositeRedis, txFunc func(dataSource interface{}) error) error {

	return nil
}

func (r *RedisDaoImpl) GetKeys(ctx context.Context, rdb *redis.Client) []string {
	return []string{}
}

func (r *RedisDaoImpl) CleanKeys(ctx context.Context, rdb *redis.Client, keys []string) error {
	return nil
}

func (r *RedisDaoImpl) Clean(ctx context.Context, rdb *redis.Client, selectFunc func([]string) []string) error {

	keys := r.RedisDao.GetKeys(ctx, rdb)
	keys = selectFunc(keys)

	r.CleanKeys(ctx, rdb, keys)

	for _, k := range keys {
		result, err := rdb.Del(ctx, k).Result()
		if err != nil {
			return err
		}

		if result < int64(len(keys)) {
			// add some warning..
		}
	}
	return nil
}
