package sinks

import (
	"fmt"
	"strings"
	"time"
	"encoding/json"
	"errors"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/output"
	"github.com/gocql/gocql"
	"github.com/oliveagle/jsonpath"
)

type CassandraConfig struct {
	Host          string            `json:"host" yaml:"host"`
	Port          int               `json:"port" yaml:"port"`
	CACertPath    string            `json:"ca_cert_path" yaml:"ca_cert_path"`
	Timeout       int               `json:"timeout" yaml:"timeout"`
	Keyspace      string            `json:"keyspace" yaml:"keyspace"`
	Table         string            `json:"table" yaml:"table"`
	FieldMappings map[string]string `json:"field_mappings" yaml:"field_mappings"`
}

func NewCassandraConfig() interface{} {
	return &CassandraConfig{
		Host:          "",
		Port:          9042,
		Timeout:       15000,
		Keyspace:      "benthos",
		Table:         "benthos",
		CACertPath:    "",
		FieldMappings: make(map[string]string),
	}
}

type Cassandra struct {
	log  log.Modular
	conf *CassandraConfig

	sess          db
	fieldMappings map[string]*jsonpath.Compiled
	statement     string

	numMessagesProcessed   metrics.StatCounter
	numValuesProcessed     metrics.StatCounter
	numInsertErrors        metrics.StatCounter
	numMessageDecodeErrors metrics.StatCounter
	cassandraInsertns      metrics.StatCounter
}

type db interface {
	NewBatch() batch
	ExecuteBatch(batch) error
	Close()
	Closed() bool
}
type gocqlWrapper struct {
	sess *gocql.Session
}

func (g *gocqlWrapper) Close() {
	g.sess.Close()
}
func (g *gocqlWrapper) Closed() bool {
	return g.sess.Closed()
}
func (g *gocqlWrapper) ExecuteBatch(b batch) error {
	return g.sess.ExecuteBatch(b.(*gocql.Batch))
}
func (g *gocqlWrapper) NewBatch() batch {
	return gocql.NewBatch(gocql.UnloggedBatch)
}

type batch interface {
	Query(string, ...interface{})
}

var (
	invalidFieldsError = errors.New("incorrect number of fields found in message")
	invalidJsonError = errors.New("invalid json input")
	invalidJsonPathError = errors.New("invalid json path configuration")
	disconnectError = errors.New("connection failed to close in allotted time")
	configError = errors.New("configuration is of incorrect type")
)


func NewCassandra(
	config *CassandraConfig,
	logger log.Modular,
	metrics metrics.Type,
) (*Cassandra,error) {

	//compile jsonpaths
	mappings := make(map[string]*jsonpath.Compiled)
	fieldsArray := make([]string, len(config.FieldMappings))
	qArray := make([]string, len(config.FieldMappings))
	i := 0
	for k, p := range config.FieldMappings {
		m,err := jsonpath.Compile(p)
		if err != nil {
			return nil, invalidJsonPathError
		}
		mappings[k] = m
		fieldsArray[i] = k
		qArray[i] = "?"
		i++
	}

	return &Cassandra{
		conf:          config,
		log:           logger.NewModule(".cassandra"),
		fieldMappings: mappings,
		statement: fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)",
			config.Table,
			strings.Join(fieldsArray, ","),
			strings.Join(qArray, ","),
		),
		numMessagesProcessed:   metrics.GetCounter("output.cassandra.send.messages"),
		numValuesProcessed:     metrics.GetCounter("output.cassandra.send.rows"),
		numInsertErrors:        metrics.GetCounter("output.cassandra.send.errors"),
		numMessageDecodeErrors: metrics.GetCounter("output.cassandra.decode.errors"),
		cassandraInsertns:      metrics.GetCounter("output.cassandra.send.insert_time_ns"),
	},nil
}

func (c *Cassandra) Connect() error {
	c.log.Infoln("Connecting to Cassandra")
	cass := gocql.NewCluster(c.conf.Host)
	cass.Timeout = time.Duration(c.conf.Timeout) * time.Millisecond
	cass.Port = c.conf.Port
	cass.Keyspace = c.conf.Keyspace
	if c.conf.CACertPath != "" {
		cass.SslOpts = &gocql.SslOptions{
			CaPath: c.conf.CACertPath,
		}
	}
	cass.ProtoVersion = 3

	if sess, err := cass.CreateSession(); err == nil {
		c.sess = &gocqlWrapper{sess: sess}
	} else {
		return err
	}
	c.log.Infoln("Connected to Cassandra")
	return nil
}

func (c *Cassandra) Write(msg types.Message) error {
	batch := c.sess.NewBatch()
	err := msg.Iter(func(i int, p types.Part) error {
		fields := make([]interface{}, len(c.fieldMappings))
		j := 0
		for _, path := range c.fieldMappings {
			var json_data interface{}
			if json.Unmarshal(p.Get(), &json_data) != nil {
				return invalidJsonError
			}
			if d, err := path.Lookup(json_data); err == nil {
				fields[j] = d
			} else {
				return err
			}
			j++
		}
		if len(fields) != len(c.fieldMappings) || len(fields) == 0 {
			return invalidFieldsError
		}
		batch.Query(c.statement, fields...)
		return nil
	})
	if err != nil {
		return err
	}
	return c.sess.ExecuteBatch(batch)
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (c *Cassandra) CloseAsync() {
	c.sess.Close()
	return
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (c *Cassandra) WaitForClose(t time.Duration) error {
	done := make(chan struct{})
	go func() {
		for !c.sess.Closed() {
			time.Sleep(10 * time.Millisecond)
		}
		done <- struct{}{}
	}()
	select {
	case <-time.After(t):
		return disconnectError
	case <-done:
	}
	return nil
}
