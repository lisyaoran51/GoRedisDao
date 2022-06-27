package candleGormDao

import (
	"time"

	"github.com/jinzhu/gorm"
	"github.com/lisyaoran51/GoRedisDao/candleDao"
)

type CandleQueryGormModel struct {
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

func (c *CandleQueryGormModel) AddCondition(field candleDao.CandleQueryModelField, condition ...interface{}) candleDao.CandleQueryModel {

	if len(condition) == 0 {
		return c
	}

	switch field {
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

func (c *CandleQueryGormModel) GetQueryChain() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.
			Scopes(c.dateBetweenScope()).
			Scopes(c.orderByListScope()).
			Scopes(c.exchangeIDEqualScope()).
			Scopes(c.dateEqualScope()).
			Scopes(c.intervalIDEqualScope()).
			Scopes(c.symbolEqualScope()).
			Scopes(c.exchangeIDInScope()).
			Scopes(c.symbolIDInScope()).
			Scopes(c.limitEqualScope())
	}
}

func (c *CandleQueryGormModel) GetDeleteChain() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.
			Scopes(c.dateBetweenScope()).
			Scopes(c.exchangeIDEqualScope()).
			Scopes(c.dateEqualScope()).
			Scopes(c.intervalIDEqualScope()).
			Scopes(c.symbolEqualScope()).
			Scopes(c.exchangeIDInScope()).
			Scopes(c.symbolIDInScope())
	}
}

func (c *CandleQueryGormModel) exchangeIDEqualScope() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if c.ExchangeCode == nil {
			return db
		}

		return db.Where("exchange_id = ?", *c.ExchangeCode)
	}
}

func (c *CandleQueryGormModel) dateBetweenScope() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if c.DateStart == nil || c.DateEnd == nil {
			return db
		}

		startFormat := (*c.DateStart).Format("2006-01-02 15:04:05")
		endFormat := (*c.DateEnd).Format("2006-01-02 15:04:05")

		return db.Where("start BETWEEN ? AND ?", startFormat, endFormat)
	}
}

func (c *CandleQueryGormModel) intervalIDEqualScope() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if c.IntervalID == nil {
			return db
		}
		return db.Where("interval_id = ?", *c.IntervalID)
	}
}

func (c *CandleQueryGormModel) symbolEqualScope() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if c.Symbol == nil {
			return db
		}
		return db.Where("symbol_id = ?", *c.Symbol)
	}
}

func (c *CandleQueryGormModel) exchangeIDInScope() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if c.ExchangeIDIn == nil {
			return db
		}
		return db.Where("exchange_id IN (?)", *c.ExchangeIDIn)
	}
}

func (c *CandleQueryGormModel) symbolIDInScope() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if c.SymbolIDIn == nil {
			return db
		}
		return db.Where("symbol_id IN (?)", *c.SymbolIDIn)
	}
}

func (c *CandleQueryGormModel) dateEqualScope() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if c.Start == nil {
			return db
		}

		return db.Where("start = ?", *c.Start)
	}
}

func (c *CandleQueryGormModel) limitEqualScope() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if c.Limit == nil {
			return db
		}
		return db.Limit(*c.Limit)
	}
}

func (c *CandleQueryGormModel) orderByListScope() func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if c.OrderByList != nil && c.OrderDirection != nil &&
			len(*c.OrderByList) > 0 && len(*c.OrderDirection) > 0 {
			order := (*c.OrderByList)[0] + " " + (*c.OrderDirection)[0]
			for i := 1; i < len(*c.OrderByList); i++ {
				order = order + ", " + (*c.OrderByList)[i] + " " + (*c.OrderDirection)[i]
			}

			return db.Order(order)
		}

		return db
	}
}
