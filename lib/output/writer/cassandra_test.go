package sinks

import (
	"fmt"
	"testing"
	"time"
	"sync/atomic"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
)

var (
	ClosedState int32
)

type mockdb struct{
	CloseTime int
}

func (_ mockdb) NewBatch() batch {
	return &mockbatch{Queries: make([]string, 0)}
}
func (m mockdb) ExecuteBatch(mb batch) error {
	fmt.Println(mb)
	return nil
}
func (m mockdb) Close() {
	go func() {
		time.Sleep(time.Duration(m.CloseTime) * time.Millisecond)
		atomic.StoreInt32(&ClosedState, 1)
	}()
}
func (m mockdb) Closed() bool {
	return atomic.LoadInt32(&ClosedState) == 1
}

type mockbatch struct {
	Queries []string
}

func (m mockbatch) String() string {
	return fmt.Sprintf("%+v", m.Queries)
}

func (m *mockbatch) Query(q string, args ...interface{}) {
	m.Queries = append(m.Queries, q+fmt.Sprintf("%+v", args))
}

func TestWrite(t *testing.T) {
	cfg := NewCassandraConfig()
	cfg.Table = "mytable"
	cfg.Keyspace = "mykeyspace"
	cfg.FieldMappings = map[string]string{
		"test": "$.test",
	}
	c := NewCassandra(cfg, log.Noop(), metrics.DudType{})
	c.sess = mockdb{}

	if err := c.Write(message.New([][]byte{[]byte(`{"test":5.0}`),[]byte(`{"test":1000}`)})); err != nil {
		t.Fatal(err)
	}
}

func TestFullDouble(t *testing.T) {
	cfg := NewCassandraConfig()
	cfg.Table = "mytable"
	cfg.Keyspace = "mykeyspace"
	cfg.FieldMappings = map[string]string{
		"indicatorId": "$.id",
		"period": "$.period",
		"timestamp": "$.time",
		"floatvalue": "$.value",
	}
	c := NewCassandra(cfg, log.Noop(), metrics.DudType{})
	c.sess = mockdb{}

	test := [][]byte{
		[]byte(`{"id":"testindicator","period":2551,"time":1536804628,"value":125.244}`),
		[]byte(`{"id":"testindicator","period":2551,"time":1536804831,"value":1001}`),
		[]byte(`{"id":"testindicator33","period":2551,"time":1535804831,"value":567.33}`),
	}

	if err := c.Write(message.New(test)); err != nil {
		t.Fatal(err)
	}
}

func TestFullInt(t *testing.T) {
	cfg := NewCassandraConfig()
	cfg.Table = "mytable"
	cfg.Keyspace = "mykeyspace"
	cfg.FieldMappings = map[string]string{
		"indicatorId": "$.id",
		"period": "$.period",
		"timestamp": "$.time",
		"intvalue": "$.value",
	}
	c := NewCassandra(cfg, log.Noop(), metrics.DudType{})
	c.sess = mockdb{}

	test := [][]byte{
		[]byte(`{"id":"testindicator","period":2551,"time":1536804628,"value":125}`),
		[]byte(`{"id":"testindicator","period":2551,"time":1536804831,"value":1001}`),
		[]byte(`{"id":"testindicator33","period":2551,"time":1535804831,"value":567}`),
	}

	if err := c.Write(message.New(test)); err != nil {
		t.Fatal(err)
	}
}

func TestFail(t *testing.T) {
	cfg := NewCassandraConfig()
	cfg.Table = "mytable"
	cfg.Keyspace = "mykeyspace"
	cfg.FieldMappings = map[string]string{
		"failure": "$.nope",
	}
	c := NewCassandra(cfg, log.Noop(), metrics.DudType{})
	c.sess = mockdb{}

	if err := c.Write(message.New([][]byte{[]byte(`{"test":5.0}`)})); err == nil {
		t.Fatal(err)
	}
}

func TestJsonParseFailure(t *testing.T) {
	cfg := NewCassandraConfig()
	cfg.Table = "mytable"
	cfg.Keyspace = "mykeyspace"
	cfg.FieldMappings = map[string]string{
		"test": "$.test",
	}
	c := NewCassandra(cfg, log.Noop(), metrics.DudType{})
	c.sess = mockdb{}

	if err := c.Write(message.New([][]byte{[]byte(`{"testtneo-INVALID`)})); err != invalidJsonError {
		t.Fatal(err)
	}
}

func TestJsonpathParseFailure(t *testing.T) {
	cfg := NewCassandraConfig()
	cfg.Table = "mytable"
	cfg.Keyspace = "mykeyspace"
	cfg.FieldMappings = map[string]string{
		"failure": "thndhoestahdnste-INVALID",
	}
	c := NewCassandra(cfg, log.Noop(), metrics.DudType{})
	c.sess = mockdb{}

	if err := c.Write(message.New([][]byte{[]byte(`{"test":5.0}`)})); err != invalidFieldsError {
		t.Fatal(err)
	}
}

func TestClose(t *testing.T) {
	cfg := NewCassandraConfig()
	cfg.Table = "mytable"
	cfg.Keyspace = "mykeyspace"
	c := NewCassandra(cfg, log.Noop(), metrics.DudType{})
	c.sess = mockdb{
		CloseTime: 100,
	}
	ClosedState = 0
	c.CloseAsync()
	if err := c.WaitForClose(500 * time.Millisecond); err != nil{
		t.Fatal(err)
	}
}
func TestCloseFail(t *testing.T) {
	cfg := NewCassandraConfig()
	cfg.Table = "mytable"
	cfg.Keyspace = "mykeyspace"
	c := NewCassandra(cfg, log.Noop(), metrics.DudType{})
	c.sess = mockdb{
		CloseTime: 5000,
	}
	ClosedState = 0
	c.CloseAsync()
	if err := c.WaitForClose(500 * time.Millisecond); err != disconnectError{
		t.Fatal(err)
	}
}
