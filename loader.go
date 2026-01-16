package mongo

import (
	"github.com/webcore-go/webcore/app/loader"
)

type MongoLoader struct {
	name string
}

func (a *MongoLoader) SetName(name string) {
	a.name = name
}

func (a *MongoLoader) Name() string {
	return a.name
}

func (l *MongoLoader) Init(args ...any) (loader.Library, error) {
	// config := args[1].(config.DatabaseConfig)

	db := &MongoDatabase{}
	err := db.Install(args...)
	if err != nil {
		return nil, err
	}

	err = db.Connect()
	if err != nil {
		return nil, err
	}

	return db, nil
}
