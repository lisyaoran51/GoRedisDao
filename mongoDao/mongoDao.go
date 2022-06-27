package mongoDao

import (
	"context"
	"fmt"
	"reflect"

	"github.com/lisyaoran51/GoRedisDao"
	"go.mongodb.org/mongo-driver/mongo"
)

type MongoDao struct{}

func (m *MongoDao) GetSourceType() string {
	source := &mongo.Database{}
	return reflect.TypeOf(source).String()
}

func (m *MongoDao) Transaction(ctx *context.Context, dataSource interface{}, txFunc func(dataSource interface{}) error) error {
	if db, ok := dataSource.(*mongo.Database); ok {
		return m.transaction(ctx, db, txFunc)
	}
	return GoRedisDao.ErrInternal
}

func (m *MongoDao) transaction(ctx *context.Context, db *mongo.Database, txFunc func(dataSource interface{}) error) error {

	var session mongo.Session
	var err error

	if session, err = db.Client().StartSession(); err != nil {
		return err
	}
	defer session.EndSession(*ctx)

	if err := session.StartTransaction(); err != nil {
		return err
	}

	if err := mongo.WithSession(*ctx, session, func(sc mongo.SessionContext) (err error) {

		defer func() {
			if err != nil {
				if abortErr := session.AbortTransaction(sc); abortErr != nil {
					fmt.Printf("failed to abort transaction")
				}
				return
			}
			if commitErr := session.CommitTransaction(sc); commitErr != nil {
				fmt.Printf("failed to commit transaction")
				err = commitErr
			}
		}()

		if err = txFunc(db); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}
	return nil
}
