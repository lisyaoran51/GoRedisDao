package candleMongoDao

import (
	"context"

	"github.com/lisyaoran51/GoRedisDao"
	"github.com/lisyaoran51/GoRedisDao/candleDao"
	"github.com/lisyaoran51/GoRedisDao/model"
	"github.com/lisyaoran51/GoRedisDao/model/mongoModel/candleMongoModel"
	"github.com/lisyaoran51/GoRedisDao/mongoDao"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type CandleMongoDao struct {
	candleDao.DaoImpl
	mongoDao.MongoDao
}

func NewCandleMongoDao() *CandleMongoDao {
	newDao := &CandleMongoDao{}
	newDao.DaoImpl.Dao = newDao
	return newDao
}

func (c *CandleMongoDao) GetCollection(db *mongo.Database) (*mongo.Collection, error) {
	const collection = "candle"
	return db.Collection(collection), nil
}

func (c *CandleMongoDao) new(ctx mongo.SessionContext, db *mongo.Database, model *model.CandleModel) (interface{}, error) {

	collection, err := c.GetCollection(db)
	if collection == nil {
		return nil, GoRedisDao.ErrInternal
	}

	candle := candleMongoModel.NewCandleMongoModel(model)
	insertOneResult, err := collection.InsertOne(ctx, candle)

	return insertOneResult, err
}

func (c *CandleMongoDao) get(ctx mongo.SessionContext, db *mongo.Database, query *CandleQueryMongoModel) (*model.CandleModel, error) {

	collection, err := c.GetCollection(db)
	if collection == nil {
		return nil, GoRedisDao.ErrInternal
	}

	var candle candleMongoModel.CandleMongoModel

	filter := query.GetFilter()
	findOneOptions := query.GetFindOneOptions()

	err = collection.FindOne(ctx, filter, findOneOptions).Decode(&candle)

	if err == mongo.ErrNoDocuments {
		return nil, nil
	}

	if err != nil {
		return nil, GoRedisDao.ErrInternal
	}

	result := candle.GetCandleModel()

	return result, nil
}

func (c *CandleMongoDao) gets(ctx mongo.SessionContext, db *mongo.Database, query *CandleQueryMongoModel) ([]*model.CandleModel, error) {

	collection, err := c.GetCollection(db)
	if collection == nil {
		return nil, GoRedisDao.ErrInternal
	}

	var candles []candleMongoModel.CandleMongoModel

	filter := query.GetFilter()
	findOptions := query.GetFindOptions()

	cursor, err := collection.Find(ctx, filter, findOptions)
	if err != nil {
		return nil, err
	}

	err = cursor.All(ctx, &candles)
	if err != nil {
		return nil, err
	}

	result := []*model.CandleModel{}
	for _, c := range candles {
		result = append(result, c.GetCandleModel())
	}

	return result, nil
}

func (c *CandleMongoDao) modify(ctx mongo.SessionContext, db *mongo.Database, model *model.CandleModel, fields []candleDao.CandleUpdateField) error {

	collection, err := c.GetCollection(db)
	if collection == nil {
		return GoRedisDao.ErrInternal
	}

	filter := bson.D{{"_id", model.ID}}

	set := bson.D{}

	if len(fields) == 0 {
		fields = append(fields, candleDao.CandleUpdateField_Open)
		fields = append(fields, candleDao.CandleUpdateField_Close)
		fields = append(fields, candleDao.CandleUpdateField_High)
		fields = append(fields, candleDao.CandleUpdateField_Low)
		fields = append(fields, candleDao.CandleUpdateField_Quantity)
	}

	for _, f := range fields {
		switch f {
		case candleDao.CandleUpdateField_Open:
			set = append(set, bson.E{"open", model.Open})
		case candleDao.CandleUpdateField_Close:
			set = append(set, bson.E{"close", model.Close})
		case candleDao.CandleUpdateField_High:
			set = append(set, bson.E{"high", model.High})
		case candleDao.CandleUpdateField_Low:
			set = append(set, bson.E{"low", model.Low})
		case candleDao.CandleUpdateField_Quantity:
			set = append(set, bson.E{"quantity", model.Quantity})
		}
	}

	update := bson.D{{"$set", set}}

	_, err = collection.UpdateOne(ctx, filter, update)

	return err
}

func (c *CandleMongoDao) delete(ctx mongo.SessionContext, db *mongo.Database, query *CandleQueryMongoModel) error {
	collection, err := c.GetCollection(db)
	if collection == nil {
		return GoRedisDao.ErrInternal
	}

	filter := query.GetFilter()
	_, err = collection.DeleteOne(ctx, filter)

	return err
}

/// ======================== REGION ========================
/// below is only for inheritance assertion. no business logic here
/// ========================================================

/// ============= Dao =============

func (c *CandleMongoDao) CreateDao() GoRedisDao.Dao {
	return NewCandleMongoDao()
}

/// ============= CandleDao =============

func (c *CandleMongoDao) New(ctx *context.Context, dataSource interface{}, model *model.CandleModel) (result interface{}, err error) {
	var session mongo.Session

	d, ok := dataSource.(*mongo.Database)
	if !ok {
		return nil, GoRedisDao.ErrInternal
	}

	// bind mongo with context
	if session, err = d.Client().StartSession(); err != nil {
		return nil, err
	}
	defer session.EndSession(*ctx)

	if err = mongo.WithSession(*ctx, session, func(sc mongo.SessionContext) error {

		if result, err = c.new(sc, d, model); err != nil {
			return err
		}
		return nil

	}); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *CandleMongoDao) Get(ctx *context.Context, dataSource interface{}, query candleDao.CandleQueryModel) (result *model.CandleModel, err error) {
	var session mongo.Session

	d, ok := dataSource.(*mongo.Database)
	if !ok {
		return nil, GoRedisDao.ErrInternal
	}

	q, ok := query.(*CandleQueryMongoModel)
	if !ok {
		return nil, GoRedisDao.ErrInternal
	}

	// bind mongo with context
	if session, err = d.Client().StartSession(); err != nil {
		return nil, err
	}
	defer session.EndSession(*ctx)

	if err = mongo.WithSession(*ctx, session, func(sc mongo.SessionContext) error {

		if result, err = c.get(sc, d, q); err != nil {
			return err
		}
		return nil

	}); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *CandleMongoDao) Gets(ctx *context.Context, dataSource interface{}, query candleDao.CandleQueryModel) (result []*model.CandleModel, err error) {
	var session mongo.Session

	d, ok := dataSource.(*mongo.Database)
	if !ok {
		return nil, GoRedisDao.ErrInternal
	}

	q, ok := query.(*CandleQueryMongoModel)
	if !ok {
		return nil, GoRedisDao.ErrInternal
	}

	// bind mongo with context
	if session, err = d.Client().StartSession(); err != nil {
		return nil, err
	}
	defer session.EndSession(*ctx)

	if err = mongo.WithSession(*ctx, session, func(sc mongo.SessionContext) error {

		if result, err = c.gets(sc, d, q); err != nil {
			return err
		}
		return nil

	}); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *CandleMongoDao) Modify(ctx *context.Context, dataSource interface{}, model *model.CandleModel, fields []candleDao.CandleUpdateField) (err error) {
	var session mongo.Session

	d, ok := dataSource.(*mongo.Database)
	if !ok {
		return GoRedisDao.ErrInternal
	}

	// bind mongo with context
	if session, err = d.Client().StartSession(); err != nil {
		return err
	}
	defer session.EndSession(*ctx)

	if err = mongo.WithSession(*ctx, session, func(sc mongo.SessionContext) error {

		if err = c.modify(sc, d, model, fields); err != nil {
			return err
		}
		return nil

	}); err != nil {
		return err
	}

	return nil
}

func (c *CandleMongoDao) Delete(ctx *context.Context, dataSource interface{}, query candleDao.CandleQueryModel) (err error) {
	var session mongo.Session

	d, ok := dataSource.(*mongo.Database)
	if !ok {
		return GoRedisDao.ErrInternal
	}

	q, ok := query.(*CandleQueryMongoModel)
	if !ok {
		return GoRedisDao.ErrInternal
	}

	// bind mongo with context
	if session, err = d.Client().StartSession(); err != nil {
		return err
	}
	defer session.EndSession(*ctx)

	if err = mongo.WithSession(*ctx, session, func(sc mongo.SessionContext) error {

		if err = c.delete(sc, d, q); err != nil {
			return err
		}
		return nil

	}); err != nil {
		return err
	}

	return nil
}

func (c *CandleMongoDao) GetQueryModel() candleDao.CandleQueryModel {
	return new(CandleQueryMongoModel)
}
