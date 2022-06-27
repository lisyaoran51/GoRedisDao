package model

import "time"

type CandleModel struct {
	ID           interface{} `gorm:"-:all"`
	ExchangeCode string      `gorm:"column:exchange_id; primary_key" json:"exchange_id"`
	Symbol       string      `gorm:"column:symbol_id; primary_key" json:"symbol_id"`
	IntervalID   string      `gorm:"column:interval_id; primary_key" json:"interval_id"`
	Start        time.Time   `gorm:"column:start; primary_key" json:"start"`
	Open         float64     `gorm:"column:open" json:"open"`
	Close        float64     `gorm:"column:close" json:"close"`
	High         float64     `gorm:"column:high" json:"high"`
	Low          float64     `gorm:"column:low" json:"low"`
	Quantity     float64     `gorm:"column:quantity" json:"quantity"`
}
