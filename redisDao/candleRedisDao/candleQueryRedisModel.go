package candleRedisDao

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/lisyaoran51/GoRedisDao/candleDao"
)

type CandleQueryRedisModel struct {
	ExchangeCode   *string    `redis:exchange_id`
	Symbol         *string    `redis:symbol_id`
	IntervalID     *string    `redis:interval_id`
	Start          *time.Time `redis:start`
	DateStart      *time.Time `redis:date_start`
	DateEnd        *time.Time `redis:date_end`
	OrderByList    *[]string  `redis:order_by`
	OrderDirection *[]string  `redis:order_direction`
	ExchangeIDIn   *[]string  `redis:exchange_id_in`
	SymbolIDIn     *[]string  `redis:symbol_id_in`
	Limit          *int       `redis:limit`
}

func (c *CandleQueryRedisModel) AddCondition(field candleDao.CandleQueryModelField, condition ...interface{}) candleDao.CandleQueryModel {

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

func (c *CandleQueryRedisModel) GetKey() string {

	key := ""

	v := reflect.ValueOf(c)
	t := reflect.TypeOf(c)
	for i := 0; i < v.NumField(); i++ {
		tag := t.Field(i).Tag.Get("redis")
		if v.Field(i).Interface() == nil {
			continue
		}
		value := v.Field(i).Interface()

		if t, ok := value.(time.Time); ok {
			value = t.Unix()
		}
		if t, ok := value.(*time.Time); ok {
			value = t.Unix()
		}

		if key != "" {
			key += ":"
		}

		valueMarshalled, _ := json.Marshal(value)
		key += tag + ":" + string(valueMarshalled)
	}
	return key
}
