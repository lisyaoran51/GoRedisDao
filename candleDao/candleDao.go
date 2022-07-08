package candleDao

import (
	"context"
	"fmt"
	"reflect"

	"github.com/lisyaoran51/GoRedisDao"
	"github.com/lisyaoran51/GoRedisDao/model"
)

type CandleUpdateField int

const (
	CandleUpdateField_None CandleUpdateField = iota
	CandleUpdateField_ID
	CandleUpdateField_ExchangeCode
	CandleUpdateField_Symbol
	CandleUpdateField_IntervalID
	CandleUpdateField_Start
	CandleUpdateField_Open
	CandleUpdateField_Close
	CandleUpdateField_High
	CandleUpdateField_Low
	CandleUpdateField_Quantity
)

var candleDaos = map[string]func() CandleDao{}

type CandleDao interface {
	GoRedisDao.Dao
	New(ctx context.Context, dataSource interface{}, model *model.CandleModel) (interface{}, error)
	Get(ctx context.Context, dataSource interface{}, query CandleQueryModel) (*model.CandleModel, error)
	Gets(ctx context.Context, dataSource interface{}, query CandleQueryModel) ([]*model.CandleModel, error)
	Modify(ctx context.Context, dataSource interface{}, model *model.CandleModel, fields []CandleUpdateField) error
	Delete(ctx context.Context, dataSource interface{}, query CandleQueryModel) error
	GetQueryModel() CandleQueryModel
}

type DaoImpl struct {
	GoRedisDao.Dao
}

func NewDaoImpl(d GoRedisDao.Dao) *DaoImpl {
	if impl, ok := d.(*DaoImpl); ok {
		impl.Dao = impl
		return impl
	}
	return nil
}

func GetDao(source interface{}) CandleDao {

	sourceType := reflect.TypeOf(source).String()

	if _, ok := candleDaos[sourceType]; !ok {
		fmt.Printf("GetCandleDao: no such dao.", sourceType)
		return nil
	}
	return candleDaos[sourceType]()
}

func (c *DaoImpl) Register() error {
	if _, ok := c.Dao.CreateDao().(CandleDao); !ok {
		return GoRedisDao.ErrInternal
	}

	candleDaos[c.Dao.GetSourceType()] = func() CandleDao {
		d := c.Dao.CreateDao()
		candleDao := d.(CandleDao)
		return candleDao
	}
	return nil
}

func (c *DaoImpl) GetModelType() string {
	model := &model.CandleModel{}
	return reflect.TypeOf(model).String()
}

func (c *DaoImpl) GetDataName() string {
	return "candle"
}
