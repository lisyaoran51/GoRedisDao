package mongoDao

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoFilter struct {
	bson.D
	options.FindOptions
	options.FindOneOptions
}

func (f *MongoFilter) Scopes(methods ...func(*MongoFilter) *MongoFilter) *MongoFilter {
	for _, m := range methods {
		f = m(f)
	}
	return f
}

func (f *MongoFilter) Add(e bson.E) *MongoFilter {
	f.D = append(f.D, e)
	return f
}

func (f *MongoFilter) Build() (bson.D, *options.FindOptions, *options.FindOneOptions) {
	return f.D, &f.FindOptions, &f.FindOneOptions
}

func (f *MongoFilter) BuildFilter() bson.D {
	return f.D
}

func (f *MongoFilter) BuildFindOptions() *options.FindOptions {
	return &f.FindOptions
}

func (f *MongoFilter) BuildFindOneOptions() *options.FindOneOptions {
	return &f.FindOneOptions
}
