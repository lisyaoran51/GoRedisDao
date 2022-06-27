package gormDao

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"runtime/debug"

	"github.com/jinzhu/gorm"
	"github.com/lisyaoran51/GoRedisDao"
)

type GormDao struct{}

func (g *GormDao) GetSourceType() string {
	source := &gorm.DB{}
	return reflect.TypeOf(source).String()
}

func (g *GormDao) Transaction(ctx *context.Context, dataSource interface{}, txFunc func(dataSource interface{}) error) error {
	if db, ok := dataSource.(*gorm.DB); ok {
		return g.transaction(ctx, db, txFunc)
	}
	return GoRedisDao.ErrInternal
}

func (g *GormDao) transaction(ctx *context.Context, db *gorm.DB, txFunc func(dataSource interface{}) error) error {
	// Obtain transaction handle.
	tx := db.BeginTx(*ctx, &sql.TxOptions{})
	var err error
	if tErr := tx.Error; tErr != nil {
		// nested transaction happened. unable to start nested transaction
		if err = txFunc(db); err != nil {
			fmt.Printf("error happened in nested transaction: %v", err)
			return err
		}
		return nil
	}
	// Defer commit / rollback before we execute transaction.
	defer func() {
		// Recover from panic.
		var recovered interface{}
		if recovered = recover(); recovered != nil {
			// Assemble log string.
			message := fmt.Sprintf("\x1b[31m%v\n[Stack Trace]\n%s\x1b[m",
				recovered, debug.Stack())

			// Record the stack trace to logging service, or if we cannot
			// find a logging from this request, use the static logging.
			fmt.Printf(message)
		}

		// Perform rollback if panic or if error is encountered.
		if recovered != nil || err != nil {
			if rerr := tx.Rollback().Error; rerr != nil {
				fmt.Printf("Failed to rollback transaction: %v", rerr)
			}
		}
	}()

	// Execute transaction.
	if err = txFunc(tx); err != nil {
		fmt.Printf("Failed to execute transaction: %v", err)
		return err
	}

	// Commit transaction.
	if err = tx.Commit().Error; err != nil {
		fmt.Printf("Failed to commit transaction: %v", err)
		return err
	}

	return nil
}
