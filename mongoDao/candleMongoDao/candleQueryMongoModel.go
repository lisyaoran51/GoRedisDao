package candleMongoDao

import (
	"time"

	"github.com/lisyaoran51/GoRedisDao/candleDao"
	"github.com/lisyaoran51/GoRedisDao/mongoDao"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CandleQueryMongoModel struct {
	ID             interface{}
	ExchangeCode   *string
	Symbol         *string
	IntervalID     *string
	Start          *time.Time
	DateStart      *time.Time
	DateEnd        *time.Time
	OrderByList    *[]string
	OrderDirection *[]string
	ExchangeIDIn   *[]string
	SymbolIDIn     *[]string
	Limit          *int
}

func (c *CandleQueryMongoModel) AddCondition(field candleDao.CandleQueryModelField, condition ...interface{}) candleDao.CandleQueryModel {

	if len(condition) == 0 {
		return c
	}

	switch field {
	case candleDao.CandleQueryModelField_ID:
		if condition[0] != nil {
			c.ID = condition[0]
		}
	case candleDao.CandleQueryModelField_ExchangeCode:
		if v, ok := condition[0].(string); ok {
			c.ExchangeCode = &v
		}
	case candleDao.CandleQueryModelField_Symbol:
		if v, ok := condition[0].(string); ok {
			c.Symbol = &v
		}
	case candleDao.CandleQueryModelField_IntervalID:
		if v, ok := condition[0].(string); ok {
			c.IntervalID = &v
		}
	case candleDao.CandleQueryModelField_Start:
		if v, ok := condition[0].(time.Time); ok {
			c.Start = &v
		}
	case candleDao.CandleQueryModelField_DateStart:
		if v, ok := condition[0].(time.Time); ok {
			c.DateStart = &v
		}
	case candleDao.CandleQueryModelField_DateEnd:
		if v, ok := condition[0].(time.Time); ok {
			c.DateEnd = &v
		}
	case candleDao.CandleQueryModelField_OrderByList:
		if v, ok := condition[0].([]string); ok {
			c.OrderByList = &v
		}
	case candleDao.CandleQueryModelField_OrderDirection:
		if v, ok := condition[0].([]string); ok {
			c.OrderDirection = &v
		}
	case candleDao.CandleQueryModelField_ExchangeIDIn:
		if v, ok := condition[0].([]string); ok {
			c.ExchangeIDIn = &v
		}
	case candleDao.CandleQueryModelField_SymbolIDIn:
		if v, ok := condition[0].([]string); ok {
			c.SymbolIDIn = &v
		}
	case candleDao.CandleQueryModelField_Limit:
		if v, ok := condition[0].(int); ok {
			c.Limit = &v
		}
		if v, ok := condition[0].(int32); ok {
			vInt := int(v)
			c.Limit = &vInt
		}
		if v, ok := condition[0].(int64); ok {
			vInt := int(v)
			c.Limit = &vInt
		}
	}
	return c
}

func (c *CandleQueryMongoModel) GetFilter() bson.D {
	filter := &mongoDao.MongoFilter{}

	return filter.
		Scopes(c.iDEqualScope()).
		Scopes(c.exchangeIDEqualScope()).
		Scopes(c.dateBetweenScope()).
		Scopes(c.dateEqualScope()).
		Scopes(c.intervalIDEqualScope()).
		Scopes(c.symbolEqualScope()).
		Scopes(c.exchangeIDInScope()).
		Scopes(c.symbolIDInScope()).
		BuildFilter()
}

func (c *CandleQueryMongoModel) GetFindOptions() *options.FindOptions {
	filter := &mongoDao.MongoFilter{}

	return filter.
		Scopes(c.limitEqualScope()).
		Scopes(c.orderByListScope()).
		BuildFindOptions()
}

func (c *CandleQueryMongoModel) GetFindOneOptions() *options.FindOneOptions {
	filter := &mongoDao.MongoFilter{}

	return filter.
		Scopes(c.orderByListScope()).
		BuildFindOneOptions()
}

// TODO: use struct tag to replace string
// ex: field, ok := reflect.TypeOf(user).Elem().FieldByName("Name")
func (c *CandleQueryMongoModel) iDEqualScope() func(*mongoDao.MongoFilter) *mongoDao.MongoFilter {
	return func(filter *mongoDao.MongoFilter) *mongoDao.MongoFilter {
		if c.ID == nil {
			return filter
		}

		return filter.Add(bson.E{"_id", c.ID})
	}
}

func (c *CandleQueryMongoModel) exchangeIDEqualScope() func(*mongoDao.MongoFilter) *mongoDao.MongoFilter {
	return func(filter *mongoDao.MongoFilter) *mongoDao.MongoFilter {
		if c.ExchangeCode == nil {
			return filter
		}

		return filter.Add(bson.E{"exchange_id", *c.ExchangeCode})
	}
}

func (c *CandleQueryMongoModel) dateBetweenScope() func(*mongoDao.MongoFilter) *mongoDao.MongoFilter {
	return func(filter *mongoDao.MongoFilter) *mongoDao.MongoFilter {
		if c.DateStart == nil || c.DateEnd == nil {
			return filter
		}

		return filter.Add(bson.E{
			"start",
			bson.D{
				{"$gte", *c.DateStart},
				{"$lte", *c.DateEnd},
			},
		})
	}
}

func (c *CandleQueryMongoModel) dateEqualScope() func(*mongoDao.MongoFilter) *mongoDao.MongoFilter {
	return func(filter *mongoDao.MongoFilter) *mongoDao.MongoFilter {
		if c.Start == nil {
			return filter
		}

		return filter.Add(bson.E{"start", *c.Start})
	}
}

func (c *CandleQueryMongoModel) intervalIDEqualScope() func(*mongoDao.MongoFilter) *mongoDao.MongoFilter {
	return func(filter *mongoDao.MongoFilter) *mongoDao.MongoFilter {
		if c.IntervalID == nil {
			return filter
		}

		return filter.Add(bson.E{"interval_id", *c.IntervalID})
	}
}
func (c *CandleQueryMongoModel) symbolEqualScope() func(*mongoDao.MongoFilter) *mongoDao.MongoFilter {
	return func(filter *mongoDao.MongoFilter) *mongoDao.MongoFilter {
		if c.Symbol == nil {
			return filter
		}

		return filter.Add(bson.E{"symbol_id", *c.Symbol})
	}
}
func (c *CandleQueryMongoModel) exchangeIDInScope() func(*mongoDao.MongoFilter) *mongoDao.MongoFilter {
	return func(filter *mongoDao.MongoFilter) *mongoDao.MongoFilter {
		if c.ExchangeIDIn == nil {
			return filter
		}

		return filter.Add(bson.E{
			"exchange_id",
			bson.D{
				{"$in", *c.ExchangeIDIn},
			},
		})
	}
}
func (c *CandleQueryMongoModel) symbolIDInScope() func(*mongoDao.MongoFilter) *mongoDao.MongoFilter {
	return func(filter *mongoDao.MongoFilter) *mongoDao.MongoFilter {
		if c.SymbolIDIn == nil {
			return filter
		}

		return filter.Add(bson.E{
			"exchange_id",
			bson.D{
				{"$in", *c.SymbolIDIn},
			},
		})
	}
}

func (c *CandleQueryMongoModel) limitEqualScope() func(*mongoDao.MongoFilter) *mongoDao.MongoFilter {
	return func(filter *mongoDao.MongoFilter) *mongoDao.MongoFilter {
		if c.Limit == nil {
			return filter
		}

		filter.FindOptions.SetLimit(int64(*c.Limit))
		return filter
	}
}
func (c *CandleQueryMongoModel) orderByListScope() func(*mongoDao.MongoFilter) *mongoDao.MongoFilter {
	return func(filter *mongoDao.MongoFilter) *mongoDao.MongoFilter {
		if c.OrderByList == nil || c.OrderDirection == nil {
			return filter
		}

		sort := bson.D{}
		for i := range *c.OrderByList {
			switch (*c.OrderDirection)[i] {
			case "":
				sort = append(sort, bson.E{(*c.OrderByList)[i], 1})
			case "ASC":
				sort = append(sort, bson.E{(*c.OrderByList)[i], 1})
			case "DESC":
				sort = append(sort, bson.E{(*c.OrderByList)[i], -1})
			}
		}
		if len(sort) > 0 {
			filter.FindOptions.SetSort(sort)
			filter.FindOneOptions.SetSort(sort)
		}

		return filter
	}
}
