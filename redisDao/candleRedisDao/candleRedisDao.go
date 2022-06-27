package candleRedisDao

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/lisyaoran51/GoRedisDao"
	"github.com/lisyaoran51/GoRedisDao/candleDao"
	"github.com/lisyaoran51/GoRedisDao/model"
	"github.com/lisyaoran51/GoRedisDao/redisDao"
)

type CandleRedisDao struct {
	candleDao.DaoImpl
	redisDao.RedisDaoImpl
}

func NewCandleRedisDao() *CandleRedisDao {
	newDao := &CandleRedisDao{}
	newDao.DaoImpl.Dao = newDao
	return newDao
}

func (c *CandleRedisDao) GetDBName() string {
	dbName := "candle"
	return dbName
}

func (c *CandleRedisDao) new(ctx context.Context, rdb *redisDao.CompositeRedis, model *model.CandleModel) (interface{}, error) {

	result, err := c.DeepNew(ctx, rdb.DB, model)

	if err != nil {
		fmt.Printf("failed\n")
		return nil, err
	}

	fmt.Printf("new: %v\n", result)

	//delete cache

	c.Clean(ctx, &rdb.Redis, func(keys []string) []string {

		selectedKeys := []string{}

		for _, k := range keys {
			if strings.Contains(k, model.Symbol) {
				selectedKeys = append(selectedKeys, k)
			}
		}

		return selectedKeys
	})

	return nil, err
}

func (c *CandleRedisDao) get(ctx context.Context, rdb *redisDao.CompositeRedis, query *CandleQueryRedisModel) (*model.CandleModel, error) {

	key := c.GetDBName() + ":" + query.GetKey() + ":1"

	result, err := rdb.Redis.Get(ctx, key).Result()

	var model *model.CandleModel

	if err == redis.Nil {
		// go to db

		model, err = c.DeepGet(ctx, rdb.DB, query)

		// save to redis
		b, err := json.Marshal(model)
		if err != nil {
			return nil, err
		}
		err = rdb.Redis.Set(ctx, key, string(b), 0).Err()
	} else {
		err = json.Unmarshal([]byte(result), &model)
		if err != nil {
			return nil, err
		}
	}

	if err != nil && err != redis.Nil {
		return nil, err
	}

	return model, nil
}

func (c *CandleRedisDao) gets(ctx context.Context, rdb *redisDao.CompositeRedis, query *CandleQueryRedisModel) ([]*model.CandleModel, error) {

	var models []*model.CandleModel

	key := c.GetDBName() + ":" + query.GetKey()

	result, err := rdb.Redis.Get(ctx, key).Result()

	if err != nil && err != redis.Nil {
		return nil, err
	}

	if err == redis.Nil {

		models, err := c.DeepGets(ctx, rdb.DB, query)

		b, err := json.Marshal(models)
		if err != nil {
			return nil, err
		}
		err = rdb.Redis.Set(ctx, key, string(b), 0).Err()
	} else {
		err = json.Unmarshal([]byte(result), &models)
	}

	return models, nil
}

func (c *CandleRedisDao) modify(ctx context.Context, rdb *redisDao.CompositeRedis, model *model.CandleModel, fields []candleDao.CandleUpdateField) error {

	err := c.DeepModify(ctx, rdb.DB, model, fields)

	//delete cache

	return err
}

func (c *CandleRedisDao) delete(ctx context.Context, rdb *redisDao.CompositeRedis, query *CandleQueryRedisModel) error {

	err := c.DeepDelete(ctx, rdb.DB, query)

	//delete cache

	return err
}

/// ======================== REGION ========================
/// below is only for inheritance assertion. no business logic here
/// ========================================================

/// ============= Dao =============

func (c *CandleRedisDao) CreateDao() GoRedisDao.Dao {
	return NewCandleRedisDao()
}

/// ============= CandleDao =============

func (c *CandleRedisDao) New(ctx context.Context, dataSource interface{}, model *model.CandleModel) (result interface{}, err error) {

	if rdb, ok := dataSource.(*redisDao.CompositeRedis); ok {
		return c.new(ctx, rdb, model)
	}

	return nil, GoRedisDao.ErrInternal
}

func (c *CandleRedisDao) Get(ctx context.Context, dataSource interface{}, query candleDao.CandleQueryModel) (result *model.CandleModel, err error) {

	q, ok := query.(*CandleQueryRedisModel)
	if !ok {
		return nil, GoRedisDao.ErrInternal
	}

	if rdb, ok := dataSource.(*redisDao.CompositeRedis); ok {
		return c.get(ctx, rdb, q)
	}

	return nil, GoRedisDao.ErrInternal
}

func (c *CandleRedisDao) Gets(ctx context.Context, dataSource interface{}, query candleDao.CandleQueryModel) (result []*model.CandleModel, err error) {

	q, ok := query.(*CandleQueryRedisModel)
	if !ok {
		return nil, GoRedisDao.ErrInternal
	}

	if rdb, ok := dataSource.(*redisDao.CompositeRedis); ok {
		return c.gets(ctx, rdb, q)
	}

	return nil, GoRedisDao.ErrInternal
}

func (c *CandleRedisDao) Modify(ctx context.Context, dataSource interface{}, model *model.CandleModel, fields []candleDao.CandleUpdateField) (err error) {

	if rdb, ok := dataSource.(*redisDao.CompositeRedis); ok {
		return c.modify(ctx, rdb, model, fields)
	}

	return GoRedisDao.ErrInternal
}

func (c *CandleRedisDao) Delete(ctx context.Context, dataSource interface{}, query candleDao.CandleQueryModel) (err error) {

	q, ok := query.(*CandleQueryRedisModel)
	if !ok {
		return GoRedisDao.ErrInternal
	}

	if rdb, ok := dataSource.(*redisDao.CompositeRedis); ok {
		return c.delete(ctx, rdb, q)
	}

	return GoRedisDao.ErrInternal
}

func (c *CandleRedisDao) GetQueryModel() candleDao.CandleQueryModel {
	return new(CandleQueryRedisModel)
}

/// ============= RedisDao =============

func (c *CandleRedisDao) DeepNew(ctx context.Context, dataSource interface{}, model *model.CandleModel) (interface{}, error) {

	dao := candleDao.GetDao(dataSource)

	result, err := dao.New(ctx, dataSource, model)

	return result, err
}

func (c *CandleRedisDao) DeepGet(ctx context.Context, dataSource interface{}, query *CandleQueryRedisModel) (*model.CandleModel, error) {

	dao := candleDao.GetDao(dataSource)

	result, err := dao.Get(ctx, dataSource, query)

	return result, err
}

func (c *CandleRedisDao) DeepGets(ctx context.Context, dataSource interface{}, query *CandleQueryRedisModel) ([]*model.CandleModel, error) {

	dao := candleDao.GetDao(dataSource)

	result, err := dao.Gets(ctx, dataSource, query)

	return result, err
}

func (c *CandleRedisDao) DeepModify(ctx context.Context, dataSource interface{}, model *model.CandleModel, fields []candleDao.CandleUpdateField) error {

	dao := candleDao.GetDao(dataSource)

	err := dao.Modify(ctx, dataSource, model, fields)

	return err
}

func (c *CandleRedisDao) DeepDelete(ctx context.Context, dataSource interface{}, query *CandleQueryRedisModel) error {

	dao := candleDao.GetDao(dataSource)

	err := dao.Delete(ctx, dataSource, query)

	return err
}
