package candleMongoModel

import (
	"time"

	"github.com/lisyaoran51/GoRedisDao/model"
)

type CandleMongoModel struct {
	ID           interface{} `bson:"_id,omitempty"`
	ExchangeCode string      `bson:"exchange_id"`
	Symbol       string      `bson:"symbol_id"`
	IntervalID   string      `bson:"interval_id"`
	Start        time.Time   `bson:"start"`
	Open         float64     `bson:"open"`
	Close        float64     `bson:"close"`
	High         float64     `bson:"high"`
	Low          float64     `bson:"low"`
	Quantity     float64     `bson:"quantity"`
}

func NewCandleMongoModel(candleModel *model.CandleModel) *CandleMongoModel {
	candleMongoModel := &CandleMongoModel{
		ID:           candleModel.ID,
		ExchangeCode: candleModel.ExchangeCode,
		Symbol:       candleModel.Symbol,
		IntervalID:   candleModel.IntervalID,
		Start:        candleModel.Start,
		Open:         candleModel.Open,
		Close:        candleModel.Close,
		High:         candleModel.High,
		Low:          candleModel.Low,
		Quantity:     candleModel.Quantity,
	}

	return candleMongoModel
}

func (c *CandleMongoModel) GetCandleModel() *model.CandleModel {
	candleModel := &model.CandleModel{
		ID:           c.ID,
		ExchangeCode: c.ExchangeCode,
		Symbol:       c.Symbol,
		IntervalID:   c.IntervalID,
		Start:        c.Start,
		Open:         c.Open,
		Close:        c.Close,
		High:         c.High,
		Low:          c.Low,
		Quantity:     c.Quantity,
	}
	return candleModel
}
