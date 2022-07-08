package redisDao

import (
	"context"
	"reflect"
	"strings"

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
	// GetKeysStringKey
	//
	// get the key of a string field in redis, which contains all the chache keys in single string, devided by ' '
	GetKeysStringKey() string
	// GetKeysSetKey
	//
	// get the key of a set in redis, which contains all the chache keys
	GetKeysSetKey() string
	AddKeys(ctx context.Context, rdb *redis.Client, keys []string) error
	GetKeys(ctx context.Context, rdb *redis.Client) ([]string, error)
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

func (r *RedisDaoImpl) AddKeys(ctx context.Context, rdb *redis.Client, keys []string) error {

	if len(keys) == 0 {
		return nil
	}

	_, err := rdb.Get(ctx, r.GetKeysStringKey()).Result()
	if err != nil && err != redis.Nil {
		return err
	}

	if err == nil {
		_, err := rdb.Del(ctx, r.GetKeysStringKey()).Result()
		if err != nil && err != redis.Nil {
			return err
		}
	}

	keysInInterface := []interface{}{}
	for _, k := range keys {
		keysInInterface = append(keysInInterface, k)
	}

	rdb.SAdd(ctx, r.GetKeysSetKey(), keysInInterface...).Result()

	return nil
}

func (r *RedisDaoImpl) GetKeysStringKey() string {
	return r.GetDataName() + ":" + "KeyString"
}

func (r *RedisDaoImpl) GetKeysSetKey() string {
	return r.GetDataName() + ":" + "KeySet"
}

func (r *RedisDaoImpl) GetKeys(ctx context.Context, rdb *redis.Client) ([]string, error) {
	keys, err := rdb.Get(ctx, r.GetKeysStringKey()).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	if err == redis.Nil {
		keysSlice := strings.Split(keys, " ")
		return keysSlice, nil
	}

	keysSlice, err := rdb.SMembers(ctx, r.GetKeysSetKey()).Result()

	return keysSlice, err
}

func (r *RedisDaoImpl) CleanKeys(ctx context.Context, rdb *redis.Client, keys []string) error {

	keysString, err := rdb.Get(ctx, r.GetKeysStringKey()).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	if err == redis.Nil {

	}

	return nil
}

func (r *RedisDaoImpl) Clean(ctx context.Context, rdb *redis.Client, selectFunc func([]string) []string) error {

	keys, err := r.Dao.(RedisDao).GetKeys(ctx, rdb)
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
