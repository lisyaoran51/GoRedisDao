package GoRedisDao

import "context"

type Dao interface {
	GetSourceType() string
	GetModelType() string
	Register() error
	CreateDao() Dao
	Transaction(ctx *context.Context, dataSource interface{}, txFunc func(dataSource interface{}) error) error
}
