package initializer

import (
	"github.com/lisyaoran51/GoRedisDao"
	"github.com/lisyaoran51/GoRedisDao/gormDao/candleGormDao"
	"github.com/lisyaoran51/GoRedisDao/mongoDao/candleMongoDao"
)

func registerDao(d GoRedisDao.Dao) error {
	return d.Register()
}

//Initialize
// please register all instance creator of dao here
// in order to achieve IoC on DAO
func Initialize() {
	registerDao(candleMongoDao.NewCandleMongoDao())
	registerDao(candleGormDao.NewCandleGormDao())
}
