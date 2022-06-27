package candleRedisModel

import (
	"time"

	"github.com/lisyaoran51/GoRedisDao/model"
)

func NewCandleRedisMap(candleModel *model.CandleModel) map[string]interface{} {

	start := candleModel.Start.Unix()

	m := map[string]interface{}{
		"exchange_id": candleModel.ExchangeCode,
		"symbol_id":   candleModel.Symbol,
		"interval_id": candleModel.IntervalID,
		"start":       start,
		"open":        candleModel.Open,
		"close":       candleModel.Close,
		"high":        candleModel.High,
		"low":         candleModel.Low,
		"quantity":    candleModel.Quantity,
	}

	return m
}

func NewCandleRedisModel(m map[string]interface{}) *model.CandleModel {

	start := time.Unix(m["start"].(int64), 0)

	model := &model.CandleModel{
		ExchangeCode: m["exchange_id"].(string),
		Symbol:       m["symbol_id"].(string),
		IntervalID:   m["interval_id"].(string),
		Start:        start,
		Open:         m["open"].(float64),
		Close:        m["close"].(float64),
		High:         m["high"].(float64),
		Low:          m["low"].(float64),
		Quantity:     m["quantity"].(float64),
	}

	return model
}
