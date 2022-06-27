package candleGormDao

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jinzhu/gorm"
	"github.com/lisyaoran51/GoRedisDao"
	"github.com/lisyaoran51/GoRedisDao/candleDao"
	"github.com/lisyaoran51/GoRedisDao/gormDao"
	"github.com/lisyaoran51/GoRedisDao/model"
)

type CandleGormDao struct {
	candleDao.DaoImpl
	gormDao.GormDao
}

func NewCandleGormDao() *CandleGormDao {
	newDao := &CandleGormDao{}
	newDao.DaoImpl.Dao = newDao
	return newDao
}

func (c *CandleGormDao) GetTable(db *gorm.DB) (*gorm.DB, error) {
	const table = "candle"
	return db.Table(table), nil
}

func (c *CandleGormDao) new(ctx *context.Context, db *gorm.DB, model *model.CandleModel) (interface{}, error) {

	db, _ = c.GetTable(db)

	err := db.Create(model).Error
	if err != nil {
		return nil, err
	}

	return model, nil
}

func (c *CandleGormDao) get(ctx *context.Context, db *gorm.DB, query *CandleQueryGormModel) (*model.CandleModel, error) {

	db, _ = c.GetTable(db)

	var candle model.CandleModel

	err := db.
		Scopes(query.GetQueryChain()).
		Scan(&candle).Error

	if gorm.IsRecordNotFoundError(err) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}
	return &candle, nil
}

func (c *CandleGormDao) gets(ctx *context.Context, db *gorm.DB, query *CandleQueryGormModel) ([]*model.CandleModel, error) {
	db, _ = c.GetTable(db)

	var candles []*model.CandleModel

	err := db.
		Scopes(query.GetQueryChain()).
		Scan(&candles).Error

	if gorm.IsRecordNotFoundError(err) {
		return []*model.CandleModel{}, nil
	}

	if err != nil {
		return nil, err
	}
	return candles, nil
}

func (c *CandleGormDao) modify(ctx *context.Context, db *gorm.DB, model *model.CandleModel, fields []candleDao.CandleUpdateField) error {

	db, _ = c.GetTable(db)

	if len(fields) == 0 {
		err := db.Save(model).Error
		return err
	}

	attrs := map[string]interface{}{}

	for _, f := range fields {
		switch f {
		case candleDao.CandleUpdateField_Open:
			attrs["open"] = model.Open
		case candleDao.CandleUpdateField_Close:
			attrs["close"] = model.Close
		case candleDao.CandleUpdateField_High:
			attrs["high"] = model.High
		case candleDao.CandleUpdateField_Low:
			attrs["low"] = model.Low
		case candleDao.CandleUpdateField_Quantity:
			attrs["quantity"] = model.Quantity
		}
	}

	err := db.
		Where("exchange_id = ?", model.ExchangeCode).
		Where("symbol_id = ?", model.Symbol).
		Where("interval_id = ?", model.IntervalID).
		Where("start = ?", model.Start).
		Updates(attrs).Error

	return err
}

func (c *CandleGormDao) delete(ctx *context.Context, db *gorm.DB, query *CandleQueryGormModel) error {

	db, _ = c.GetTable(db)

	err := db.
		Scopes(query.GetDeleteChain()).
		Delete(&model.CandleModel{}).Error

	return err
}

/// ======================== REGION ========================
/// below is only for inheritance assertion. no business logic here
/// ========================================================

/// ============= Dao =============

func (c *CandleGormDao) CreateDao() GoRedisDao.Dao {
	return NewCandleGormDao()
}

/// ============= CandleDao =============

func (c *CandleGormDao) New(ctx *context.Context, dataSource interface{}, model *model.CandleModel) (result interface{}, err error) {
	d, ok := dataSource.(*gorm.DB)
	if !ok {
		return nil, GoRedisDao.ErrInternal
	}

	// bind db with context
	if tx := d.BeginTx(*ctx, &sql.TxOptions{}); tx.Error == nil {
		defer func() {
			if err != nil {
				if rollbackErr := tx.Rollback().Error; rollbackErr != nil {
					fmt.Printf("failed to rollback %s", rollbackErr)
				}
				return
			}
			err = tx.Commit().Error
		}()

		return c.new(ctx, tx, model)
	}

	// nested transaction happened. unable to set context
	return c.new(ctx, d, model)
}

func (c *CandleGormDao) Get(ctx *context.Context, dataSource interface{}, query candleDao.CandleQueryModel) (result *model.CandleModel, err error) {
	d, ok := dataSource.(*gorm.DB)
	if !ok {
		return nil, GoRedisDao.ErrInternal
	}

	q, ok := query.(*CandleQueryGormModel)
	if !ok {
		return nil, GoRedisDao.ErrInternal
	}

	// bind db with context
	if tx := d.BeginTx(*ctx, &sql.TxOptions{}); tx.Error == nil {
		defer func() {
			if err != nil {
				if rollbackErr := tx.Rollback().Error; rollbackErr != nil {
					fmt.Printf("failed to rollback %s", rollbackErr)
				}
				return
			}
			err = tx.Commit().Error
		}()

		return c.get(ctx, tx, q)
	}

	// nested transaction happened. unable to set context
	return c.get(ctx, d, q)
}

func (c *CandleGormDao) Gets(ctx *context.Context, dataSource interface{}, query candleDao.CandleQueryModel) (result []*model.CandleModel, err error) {
	d, ok := dataSource.(*gorm.DB)
	if !ok {
		return nil, GoRedisDao.ErrInternal
	}

	q, ok := query.(*CandleQueryGormModel)
	if !ok {
		return nil, GoRedisDao.ErrInternal
	}

	// bind db with context
	if tx := d.BeginTx(*ctx, &sql.TxOptions{}); tx.Error == nil {
		defer func() {
			if err != nil {
				if rollbackErr := tx.Rollback().Error; rollbackErr != nil {
					fmt.Printf("failed to rollback %s", rollbackErr)
				}
				return
			}
			err = tx.Commit().Error

		}()

		return c.gets(ctx, tx, q)
	}

	// nested transaction happened. unable to set context
	return c.gets(ctx, d, q)
}

func (c *CandleGormDao) Modify(ctx *context.Context, dataSource interface{}, model *model.CandleModel, fields []candleDao.CandleUpdateField) (err error) {
	d, ok := dataSource.(*gorm.DB)
	if !ok {
		return GoRedisDao.ErrInternal
	}

	// bind db with context
	if tx := d.BeginTx(*ctx, &sql.TxOptions{}); tx.Error == nil {
		defer func() {
			if err != nil {
				if rollbackErr := tx.Rollback().Error; rollbackErr != nil {
					fmt.Printf("failed to rollback %s", rollbackErr)
				}
				return
			}
			err = tx.Commit().Error
		}()

		return c.modify(ctx, tx, model, fields)
	}

	// nested transaction happened. unable to set context
	return c.modify(ctx, d, model, fields)
}

func (c *CandleGormDao) Delete(ctx *context.Context, dataSource interface{}, query candleDao.CandleQueryModel) (err error) {
	d, ok := dataSource.(*gorm.DB)
	if !ok {
		return GoRedisDao.ErrInternal
	}

	q, ok := query.(*CandleQueryGormModel)
	if !ok {
		return GoRedisDao.ErrInternal
	}

	// bind db with context
	if tx := d.BeginTx(*ctx, &sql.TxOptions{}); tx.Error == nil {
		defer func() {
			if err != nil {
				if rollbackErr := tx.Rollback().Error; rollbackErr != nil {
					fmt.Printf("failed to rollback %s", rollbackErr)
				}
				return
			}
			err = tx.Commit().Error
		}()

		return c.delete(ctx, tx, q)
	}

	// nested transaction happened. unable to set context
	return c.delete(ctx, d, q)
}

func (c *CandleGormDao) GetQueryModel() candleDao.CandleQueryModel {
	return new(CandleQueryGormModel)
}
