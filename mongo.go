package mongo

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"strings"

	"github.com/semanggilab/webcore-go/app/config"
	"github.com/semanggilab/webcore-go/app/loader"
	"github.com/semanggilab/webcore-go/app/logger"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoDB interface {
	loader.IDatabase
}

// MongoDatabase implements Database for MongoDB
type MongoDatabase struct {
	conn        *mongo.Client
	context     context.Context
	config      config.DatabaseConfig
	collections map[string]*mongo.Collection
}

func (m *MongoDatabase) GetConnection() any {
	return m.conn
}

func (m *MongoDatabase) GetDriver() string {
	return m.config.Driver
}

func (m *MongoDatabase) GetName() string {
	return "MongoDB"
}

// Install library
func (d *MongoDatabase) Install(args ...any) error {
	d.context = args[0].(context.Context)
	d.config = args[1].(config.DatabaseConfig)
	if len(args) > 2 {
		client := args[2].(*mongo.Client)
		if client != nil {
			// wrap existing connection
			d.conn = client
		}
	}

	d.collections = make(map[string]*mongo.Collection)
	return nil
}

func (m *MongoDatabase) Connect() error {
	// Connection is already established in Install
	if m.conn != nil {
		return nil
	}

	driver := m.config.Driver

	// Build connection string with authentication
	authSource := m.config.Name
	if authSource == "" {
		authSource = "admin"
	}

	var host string
	if strings.Contains(m.config.Host, ",") || m.config.Port == 0 {
		host = m.config.Host
	} else {
		host = fmt.Sprintf("%s:%d", m.config.Host, m.config.Port)
	}

	var connectionString string
	if m.config.User != "" && m.config.Password != "" {
		connectionString = fmt.Sprintf("%s://%s:%s@%s/",
			driver,
			m.config.User,
			m.config.Password,
			host,
		)
	} else {
		connectionString = fmt.Sprintf("%s://%s/",
			driver,
			host,
		)
	}

	if len(m.config.Attributes) > 0 {
		queryParams := []string{}
		for key, value := range m.config.Attributes {
			queryParams = append(queryParams, fmt.Sprintf("%s=%s", key, url.QueryEscape(value)))
		}
		if len(queryParams) > 0 {
			connectionString += "?" + strings.Join(queryParams, "&")
		}
	}

	logger.Debug("Attempting to connect to MongoDB with", "URI", connectionString)

	// Create client options
	clientOpts := options.Client().
		SetRetryWrites(true).
		SetRetryReads(true).
		SetMinPoolSize(5).
		SetMaxConnecting(100).
		ApplyURI(connectionString)

	// Connect to MongoDB
	client, err := mongo.Connect(m.context, clientOpts)
	if err != nil {
		logger.Error("Failed to connect to MongoDB", "error", err)
		return nil
	}

	// Ping the database to verify connection
	err = client.Ping(m.context, readpref.Primary())
	if err != nil {
		logger.Error("Failed to ping MongoDB", "error", err)
		return err
	}

	m.conn = client
	logger.Info("Successfully connected to MongoDB")
	return nil
}

func (m *MongoDatabase) Disconnect() error {
	if m.conn != nil {
		err := m.conn.Disconnect(m.context)
		if err == nil {
			logger.Info("Successfully disconnected from " + m.GetName())
		}
		return err
	}
	return nil
}

// Connect establishes a database connection
func (d *MongoDatabase) Uninstall() error {
	// Connection is already established in NewSQLDatabase
	return nil
}

func (m *MongoDatabase) Ping(ctx context.Context) error {
	if m.conn != nil {
		return m.conn.Ping(ctx, nil)
	}
	return nil
}

func (m *MongoDatabase) Watch(ctx context.Context, table string) *mongo.ChangeStream {
	collection := m.GetCollection(table)

	// Create change stream options
	changeStreamOptions := options.ChangeStream()
	changeStreamOptions.SetFullDocument(options.UpdateLookup)

	// Create change stream
	changeStream, err := collection.Watch(ctx, []bson.M{}, changeStreamOptions)
	if err != nil {
		logger.Error("Gagal membuat change stream", "error", err, "collection", table)
		return nil
	}
	defer changeStream.Close(ctx)

	return changeStream
}

func (m *MongoDatabase) RestartWatch(ctx context.Context, table string, changeStream *mongo.ChangeStream) (*mongo.ChangeStream, error) {
	changeStream.Close(ctx)

	collection := m.GetCollection(table)

	// Create change stream options
	changeStreamOptions := options.ChangeStream()
	changeStreamOptions.SetFullDocument(options.UpdateLookup)

	changeStream, err := collection.Watch(ctx, []bson.M{}, changeStreamOptions)
	if err != nil {
		logger.Error("Gagal membuat ulang change stream", "error", err)
	}
	return changeStream, err
}

func (m *MongoDatabase) Count(ctx context.Context, table string, filter loader.DbMap) (int64, error) {
	collection := m.GetCollection(table)
	if collection == nil {
		return 0, fmt.Errorf("collection %s not found", table)
	}

	mfilter := bson.M{}
	copyToBsonMap(filter, &mfilter)

	return collection.CountDocuments(ctx, mfilter)
}

func (m *MongoDatabase) Find(ctx context.Context, table string, column []string, filter loader.DbMap, sort map[string]int, limit int64, skip int64) ([]loader.DbMap, error) {
	collection := m.GetCollection(table)
	if collection == nil {
		return nil, fmt.Errorf("collection %s not found", table)
	}

	// Create projection if columns are specified
	projection := bson.M{}
	if len(column) > 0 {
		// Create projection
		for _, col := range column {
			projection[col] = 1
		}
	}

	// Build find options
	findOptions := options.Find()
	if len(projection) > 0 {
		findOptions.SetProjection(projection)
	}

	if len(sort) > 0 {
		sortBson := bson.M{}
		for field, order := range sort {
			if order == 1 {
				sortBson[field] = 1
			} else {
				sortBson[field] = -1
			}
		}
		findOptions.SetSort(sortBson)
	}

	if limit > 0 {
		findOptions.SetLimit(limit)
	}

	if skip > 0 {
		findOptions.SetSkip(skip)
	}

	mfilter := bson.M{}
	copyToBsonMap(filter, &mfilter)
	cursor, err := collection.Find(ctx, mfilter, findOptions)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []loader.DbMap
	if err = cursor.All(ctx, &results); err != nil {
		return nil, err
	}
	return results, nil
}

func (m *MongoDatabase) FindOne(ctx context.Context, result any, table string, column []string, filter loader.DbMap, sort map[string]int) error {
	collection := m.GetCollection(table)
	if collection == nil {
		return fmt.Errorf("collection %s not found", table)
	}

	// Create projection if columns are specified
	projection := bson.M{}
	if len(column) > 0 {
		// Create projection
		for _, col := range column {
			projection[col] = 1
		}
	}

	// Build find options
	findOptions := options.FindOne()
	if len(projection) > 0 {
		findOptions.SetProjection(projection)
	}

	if len(sort) > 0 {
		sortBson := bson.M{}
		for field, order := range sort {
			if order == 1 {
				sortBson[field] = 1
			} else {
				sortBson[field] = -1
			}
		}
		findOptions.SetSort(sortBson)
	}

	mfilter := bson.M{}
	copyToBsonMap(filter, &mfilter)

	err := collection.FindOne(ctx, mfilter, findOptions).Decode(result)
	if err != nil {
		return err
	}
	return nil
}

func (m *MongoDatabase) InsertOne(ctx context.Context, table string, data any) (any, error) {
	collection := m.GetCollection(table)
	if collection == nil {
		return nil, fmt.Errorf("collection %s not found", table)
	}

	_, err := collection.InsertOne(ctx, data)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (m *MongoDatabase) Update(ctx context.Context, table string, filter loader.DbMap, data any) (int64, error) {
	collection := m.GetCollection(table)
	if collection == nil {
		return 0, fmt.Errorf("collection %s not found", table)
	}

	mfilter := bson.M{}
	copyToBsonMap(filter, &mfilter)

	result, err := collection.UpdateMany(ctx, mfilter, bson.M{"$set": data})
	if err != nil {
		return 0, err
	}
	return result.MatchedCount, nil
}

func (m *MongoDatabase) UpdateOne(ctx context.Context, table string, filter loader.DbMap, data any) (int64, error) {
	collection := m.GetCollection(table)
	if collection == nil {
		return 0, fmt.Errorf("collection %s not found", table)
	}

	mfilter := bson.M{}
	copyToBsonMap(filter, &mfilter)

	result, err := collection.UpdateOne(ctx, mfilter, bson.M{"$set": data})
	if err != nil {
		return 0, err
	}
	return result.MatchedCount, nil
}

func (m *MongoDatabase) Delete(ctx context.Context, table string, filter loader.DbMap) (any, error) {
	collection := m.GetCollection(table)
	if collection == nil {
		return nil, fmt.Errorf("collection %s not found", table)
	}

	mfilter := bson.M{}
	copyToBsonMap(filter, &mfilter)
	result, err := collection.DeleteMany(ctx, mfilter)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *MongoDatabase) DeleteOne(ctx context.Context, table string, filter loader.DbMap) (any, error) {
	collection := m.GetCollection(table)
	if collection == nil {
		return nil, fmt.Errorf("collection %s not found", table)
	}

	mfilter := bson.M{}
	copyToBsonMap(filter, &mfilter)
	result, err := collection.DeleteOne(ctx, mfilter)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *MongoDatabase) GetCollection(collectionName string) *mongo.Collection {
	if collection, ok := m.collections[collectionName]; ok {
		return collection
	}
	collection := m.conn.Database(m.config.Name).Collection(collectionName)
	return collection
}

func copyToBsonMap(src loader.DbMap, dst *bson.M) any {
	for k, v := range src {
		vType := reflect.TypeOf(v)
		switch vType.Kind() {
		case reflect.Map:
			m := bson.M{}
			copyToBsonMap(v.(loader.DbMap), &m)
			(*dst)[k] = m
		case reflect.Slice:
			arr := reflect.ValueOf(v)
			result := make([]any, arr.Len())
			for i := 0; i < arr.Len(); i++ {
				item := arr.Index(i).Interface()
				iType := reflect.TypeOf(item)
				if iType.Kind() == reflect.Map {
					m := bson.M{}
					copyToBsonMap(item.(loader.DbMap), &m)
					result[i] = m
				} else {
					result[i] = item
				}
			}
			(*dst)[k] = result
		default:
			(*dst)[k] = v
		}
	}

	return dst
}
