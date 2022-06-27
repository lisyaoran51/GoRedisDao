package candleDao

type CandleQueryModelField int

const (
	CandleQueryModelField_None CandleQueryModelField = iota

	// type:primitive.ObjectID
	CandleQueryModelField_ID

	// type:string
	CandleQueryModelField_ExchangeCode

	// type:string
	CandleQueryModelField_Symbol

	// type:string
	CandleQueryModelField_IntervalID

	// type:time.Time
	CandleQueryModelField_Start

	// type:time.Time
	CandleQueryModelField_DateStart

	// type:time.Time
	CandleQueryModelField_DateEnd

	// type:[]string
	CandleQueryModelField_OrderByList

	// type:[]string
	CandleQueryModelField_OrderDirection

	// type:[]string
	CandleQueryModelField_ExchangeIDIn

	// type:[]string
	CandleQueryModelField_SymbolIDIn

	// type:int, int32, int64
	CandleQueryModelField_Limit
)

type CandleQueryModel interface {
	AddCondition(field CandleQueryModelField, condition ...interface{}) CandleQueryModel
}
