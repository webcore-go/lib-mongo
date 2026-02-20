package mongo

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mongodb"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/webcore-go/webcore/infra/config"
	"github.com/webcore-go/webcore/infra/logger"
	"github.com/webcore-go/webcore/port"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoDB interface {
	port.IDatabase
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
	scheme := m.config.Scheme
	if scheme == "" {
		scheme = driver
	}

	port := m.config.Port
	srv := false
	if strings.Contains(scheme, "+srv") {
		srv = true
		if port == 27017 {
			port = 0
		}
	}

	// Build connection string with authentication
	authSource := m.config.Name
	if authSource == "" {
		authSource = "admin"
	}

	var host string
	if strings.Contains(m.config.Host, ",") || port == 0 {
		host = m.config.Host
	} else {
		host = fmt.Sprintf("%s:%d", m.config.Host, port)
	}

	var connectionString string
	if m.config.User != "" && m.config.Password != "" {
		if srv {
			connectionString = fmt.Sprintf("%s://%s:%s@%s/%s",
				scheme,
				m.config.User,
				m.config.Password,
				host,
				m.config.Name,
			)
		} else {
			connectionString = fmt.Sprintf("%s://%s:%s@%s/",
				scheme,
				m.config.User,
				m.config.Password,
				host,
			)
		}
	} else {
		connectionString = fmt.Sprintf("%s://%s/",
			scheme,
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

func (m *MongoDatabase) Count(ctx context.Context, table string, filter []port.DbExpression) (int64, error) {
	collection := m.GetCollection(table)
	if collection == nil {
		return 0, fmt.Errorf("collection %s not found", table)
	}

	mfilter := bson.M{}
	buildWhereClause(filter, &mfilter)

	return collection.CountDocuments(ctx, mfilter)
}

func (m *MongoDatabase) Find(ctx context.Context, results any, table string, column []string, filter []port.DbExpression, sort map[string]int, limit int64, skip int64) error {
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
	buildWhereClause(filter, &mfilter)
	cursor, err := collection.Find(ctx, mfilter, findOptions)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	// var results []port.DbMap
	if err = cursor.All(ctx, &results); err != nil {
		return err
	}
	return nil
}

func (m *MongoDatabase) FindOne(ctx context.Context, result any, table string, column []string, filter []port.DbExpression, sort map[string]int) error {
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
	buildWhereClause(filter, &mfilter)

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

func (m *MongoDatabase) Update(ctx context.Context, table string, filter []port.DbExpression, data any) (int64, error) {
	collection := m.GetCollection(table)
	if collection == nil {
		return 0, fmt.Errorf("collection %s not found", table)
	}

	mfilter := bson.M{}
	buildWhereClause(filter, &mfilter)

	result, err := collection.UpdateMany(ctx, mfilter, bson.M{"$set": data})
	if err != nil {
		return 0, err
	}
	return result.MatchedCount, nil
}

func (m *MongoDatabase) UpdateOne(ctx context.Context, table string, filter []port.DbExpression, data any) (int64, error) {
	collection := m.GetCollection(table)
	if collection == nil {
		return 0, fmt.Errorf("collection %s not found", table)
	}

	mfilter := bson.M{}
	buildWhereClause(filter, &mfilter)

	result, err := collection.UpdateOne(ctx, mfilter, bson.M{"$set": data})
	if err != nil {
		return 0, err
	}
	return result.MatchedCount, nil
}

func (m *MongoDatabase) Delete(ctx context.Context, table string, filter []port.DbExpression) (int64, error) {
	collection := m.GetCollection(table)
	if collection == nil {
		return 0, fmt.Errorf("collection %s not found", table)
	}

	mfilter := bson.M{}
	buildWhereClause(filter, &mfilter)
	result, err := collection.DeleteMany(ctx, mfilter)
	if err != nil {
		return 0, err
	}
	return result.DeletedCount, nil
}

func (m *MongoDatabase) DeleteOne(ctx context.Context, table string, filter []port.DbExpression) (int64, error) {
	collection := m.GetCollection(table)
	if collection == nil {
		return 0, fmt.Errorf("collection %s not found", table)
	}

	mfilter := bson.M{}
	buildWhereClause(filter, &mfilter)
	result, err := collection.DeleteOne(ctx, mfilter)
	if err != nil {
		return 0, err
	}
	return result.DeletedCount, nil
}

func (m *MongoDatabase) GetCollection(collectionName string) *mongo.Collection {
	if collection, ok := m.collections[collectionName]; ok {
		return collection
	}
	collection := m.conn.Database(m.config.Name).Collection(collectionName)
	return collection
}

// StartMigration runs MongoDB migrations using golang-migrate
func (m *MongoDatabase) StartMigration(service string, command string, dir string, args []string) error {
	if m.conn == nil {
		return fmt.Errorf("database not connected")
	}

	// Set the collection name for migration logs based on service
	var collectionName string
	if service != "" {
		collectionName = "__migration_" + service + "_logs"
	} else {
		collectionName = "__migration_webcore_logs"
	}

	// Build MongoDB connection string with migration collection parameter
	driver := m.config.Driver
	scheme := m.config.Scheme
	if scheme == "" {
		scheme = driver
	}

	port := m.config.Port
	srv := false
	if strings.Contains(scheme, "+srv") {
		srv = true
		if port == 27017 {
			port = 0
		}
	}

	authSource := m.config.Name
	if authSource == "" {
		authSource = "admin"
	}

	var host string
	if strings.Contains(m.config.Host, ",") || port == 0 {
		host = m.config.Host
	} else {
		host = fmt.Sprintf("%s:%d", m.config.Host, port)
	}

	var connectionString string
	if m.config.User != "" && m.config.Password != "" {
		if srv {
			connectionString = fmt.Sprintf("%s://%s:%s@%s/%s",
				scheme,
				m.config.User,
				m.config.Password,
				host,
				m.config.Name,
			)
		} else {
			connectionString = fmt.Sprintf("%s://%s:%s@%s/%s",
				scheme,
				m.config.User,
				m.config.Password,
				host,
				m.config.Name,
			)
		}
	} else {
		connectionString = fmt.Sprintf("%s://%s/%s",
			scheme,
			host,
			m.config.Name,
		)
	}

	// Add migration collection parameter
	connectionString += fmt.Sprintf("?x-migrations-collection=%s", collectionName)

	// Create migration instance
	migrations, err := migrate.New(
		"file://"+dir,
		connectionString,
	)
	if err != nil {
		log.Fatalf("Failed to create migration instance: %v", err)
		return err
	}
	defer migrations.Close()

	// Execute the migration command
	switch command {
	case "up":
		if err := migrations.Up(); err != nil && err != migrate.ErrNoChange {
			log.Fatalf("Failed to run migrations up: %v", err)
			return err
		}
		if err == migrate.ErrNoChange {
			logger.Info("No new migrations to apply")
		} else {
			logger.Info("MongoDB migrations up completed successfully")
		}
	case "down":
		// Parse version if provided
		var version uint
		if len(args) > 0 {
			_, err := fmt.Sscanf(args[0], "%d", &version)
			if err != nil {
				log.Fatalf("Invalid version number: %v", err)
				return err
			}
			if err := migrations.Migrate(version); err != nil {
				log.Fatalf("Failed to run migrations down to version %d: %v", version, err)
				return err
			}
			logger.Info(fmt.Sprintf("MongoDB migrations down to version %d completed successfully", version))
		} else {
			// Rollback one migration
			if err := migrations.Steps(-1); err != nil && err != migrate.ErrNoChange {
				log.Fatalf("Failed to run migrations down: %v", err)
				return err
			}
			if err == migrate.ErrNoChange {
				logger.Info("No migrations to rollback")
			} else {
				logger.Info("MongoDB migrations down (one step) completed successfully")
			}
		}
	case "status":
		// Get migration version
		version, dirty, err := migrations.Version()
		if err != nil && err != migrate.ErrNilVersion {
			log.Fatalf("Failed to get migration status: %v", err)
			return err
		}

		fmt.Println("\nMigration Status:")
		fmt.Println("================")
		if err == migrate.ErrNilVersion {
			fmt.Println("No migrations have been applied yet")
		} else {
			fmt.Printf("Current version: %d\n", version)
			if dirty {
				fmt.Println("Status: DIRTY (migration failed)")
			} else {
				fmt.Println("Status: OK")
			}
		}
		fmt.Println()
	case "version":
		version, dirty, err := migrations.Version()
		if err != nil && err != migrate.ErrNilVersion {
			log.Fatalf("Failed to get migration version: %v", err)
			return err
		}

		if err == migrate.ErrNilVersion {
			fmt.Println("No migrations have been applied yet")
		} else {
			fmt.Printf("Current version: %d (dirty: %v)\n", version, dirty)
		}
	case "force":
		if len(args) < 1 {
			log.Fatal("Usage: force <version>")
			return fmt.Errorf("version is required")
		}
		var version int
		_, err := fmt.Sscanf(args[0], "%d", &version)
		if err != nil {
			log.Fatalf("Invalid version number: %v", err)
			return err
		}
		if err := migrations.Force(version); err != nil {
			log.Fatalf("Failed to force migration version: %v", err)
			return err
		}
		logger.Info(fmt.Sprintf("Migration version forced to %d", version))
	default:
		log.Fatalf("Unknown migration command: %s. Available commands: up, down, status, version, force", command)
		return fmt.Errorf("unknown migration command: %s", command)
	}

	return nil
}

func buildWhereClause(scr []port.DbExpression, dst *bson.M) {
	if len(scr) == 0 {
		return
	}

	for _, value := range scr {
		if value.Op != "" {
			switch value.Op {
			case "IN":
				if len(value.Args) > 0 {
					(*dst)[value.Expr] = bson.M{"$in": value.Args}
				}
			case "NOT IN":
				if len(value.Args) > 0 {
					(*dst)[value.Expr] = bson.M{"$nin": value.Args}
				}
			case "ANY =":
				if len(value.Args) > 0 {
					(*dst)[value.Expr] = bson.M{"$elemMatch": bson.M{"$eq": value.Args[0]}}
				}
			case "ANY !=":
				if len(value.Args) > 0 {
					(*dst)[value.Expr] = bson.M{"$elemMatch": bson.M{"$ne": value.Args[0]}}
				}
			case "ANY >":
				if len(value.Args) > 0 {
					(*dst)[value.Expr] = bson.M{"$elemMatch": bson.M{"$gt": value.Args[0]}}
				}
			case "ANY >=":
				if len(value.Args) > 0 {
					(*dst)[value.Expr] = bson.M{"$elemMatch": bson.M{"$gte": value.Args[0]}}
				}
			case "ANY <":
				if len(value.Args) > 0 {
					(*dst)[value.Expr] = bson.M{"$elemMatch": bson.M{"$lt": value.Args[0]}}
				}
			case "ANY <=":
				if len(value.Args) > 0 {
					(*dst)[value.Expr] = bson.M{"$elemMatch": bson.M{"$lte": value.Args[0]}}
				}
			case "ANY":
				if len(value.Args) > 0 {
					(*dst)[value.Expr] = bson.M{"$in": value.Args}
				}
			case "NOT ANY":
				if len(value.Args) > 0 {
					(*dst)[value.Expr] = bson.M{"$nin": value.Args}
				}
			case "ANY LIKE":
				if len(value.Args) > 0 {
					pattern := value.Args[0].(string)
					(*dst)[value.Expr] = bson.M{"$elemMatch": bson.M{"$regex": pattern}}
				}
			case "ANY ILIKE":
				if len(value.Args) > 0 {
					pattern := value.Args[0].(string)
					(*dst)[value.Expr] = bson.M{"$elemMatch": bson.M{"$regex": pattern, "$options": "i"}}
				}
			case "LIKE":
				if len(value.Args) > 0 {
					pattern := value.Args[0].(string)
					(*dst)[value.Expr] = bson.M{"$regex": pattern}
				}
			case "ILIKE":
				if len(value.Args) > 0 {
					pattern := value.Args[0].(string)
					(*dst)[value.Expr] = bson.M{"$regex": pattern, "$options": "i"}
				}
			case "GROUP_OR":
				ln := len(value.Args)
				if ln > 0 {
					conditions := make([]bson.M, ln)
					for i, arg := range value.Args {
						if cond, ok := arg.(port.DbExpression); ok {
							subFilter := bson.M{}
							buildWhereClause([]port.DbExpression{cond}, &subFilter)
							conditions[i] = subFilter
						}
					}
					(*dst)["$or"] = conditions
				}
			case "GROUP_AND":
				ln := len(value.Args)
				if ln > 0 {
					conditions := make([]bson.M, ln)
					for i, arg := range value.Args {
						if cond, ok := arg.(port.DbExpression); ok {
							subFilter := bson.M{}
							buildWhereClause([]port.DbExpression{cond}, &subFilter)
							conditions[i] = subFilter
						}
					}
					(*dst)["$and"] = conditions
				}
			default:
				// Handle comparison operators (=, !=, >, >=, <, <=)
				if len(value.Args) > 0 {
					switch value.Args[0] {
					case nil:
						(*dst)[value.Expr] = bson.M{"$exists": false}
					case true:
						(*dst)[value.Expr] = true
					case false:
						(*dst)[value.Expr] = false
					default:
						switch value.Op {
						case "=":
							(*dst)[value.Expr] = value.Args[0]
						case "!=":
							(*dst)[value.Expr] = bson.M{"$ne": value.Args[0]}
						case ">":
							(*dst)[value.Expr] = bson.M{"$gt": value.Args[0]}
						case ">=":
							(*dst)[value.Expr] = bson.M{"$gte": value.Args[0]}
						case "<":
							(*dst)[value.Expr] = bson.M{"$lt": value.Args[0]}
						case "<=":
							(*dst)[value.Expr] = bson.M{"$lte": value.Args[0]}
						default:
							// Default to equality
							(*dst)[value.Expr] = value.Args[0]
						}
					}
				}
			}
		} else {
			// Handle expressions without operator (default to equality)
			if len(value.Args) > 0 {
				switch value.Args[0] {
				case nil:
					(*dst)[value.Expr] = bson.M{"$exists": false}
				case true:
					(*dst)[value.Expr] = true
				case false:
					(*dst)[value.Expr] = false
				default:
					// Check if Expr contains a placeholder
					if strings.Contains(value.Expr, "?") {
						// For raw expressions, we can't easily convert to MongoDB
						// This is a limitation - raw SQL expressions won't work with MongoDB
						logger.Warn("Raw SQL expressions are not supported in MongoDB", "expr", value.Expr)
					} else {
						// Default to equality
						(*dst)[value.Expr] = value.Args[0]
					}
				}
			}
		}
	}
}
