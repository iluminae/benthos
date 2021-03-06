package batch

import (
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestPolicyBasic(t *testing.T) {
	conf := NewPolicyConfig()
	conf.Count = 2
	conf.ByteSize = 0

	pol, err := NewPolicy(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if v := pol.UntilNext(); v >= 0 {
		t.Errorf("Non-negative period: %v", v)
	}

	if exp, act := 0, pol.Count(); exp != act {
		t.Errorf("Wrong count: %v != %v", act, exp)
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}

	if pol.Add(message.NewPart(exp[0])) {
		t.Error("Unexpected batch")
	}
	if exp, act := 1, pol.Count(); exp != act {
		t.Errorf("Wrong count: %v != %v", act, exp)
	}
	if !pol.Add(message.NewPart(exp[1])) {
		t.Error("Expected batch")
	}
	if exp, act := 2, pol.Count(); exp != act {
		t.Errorf("Wrong count: %v != %v", act, exp)
	}

	msg := pol.Flush()
	if !reflect.DeepEqual(exp, message.GetAllBytes(msg)) {
		t.Errorf("Wrong result: %s != %s", message.GetAllBytes(msg), exp)
	}
	if exp, act := 0, pol.Count(); exp != act {
		t.Errorf("Wrong count: %v != %v", act, exp)
	}

	if msg = pol.Flush(); msg != nil {
		t.Error("Non-nil empty flush")
	}
}

func TestPolicyPeriod(t *testing.T) {
	conf := NewPolicyConfig()
	conf.Period = "300ms"

	pol, err := NewPolicy(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	if pol.Add(message.NewPart(nil)) {
		t.Error("Unexpected batch ready")
	}

	if v := pol.UntilNext(); v >= (time.Millisecond*300) || v < (time.Millisecond*100) {
		t.Errorf("Wrong period: %v", v)
	}

	<-time.After(time.Millisecond * 500)
	if v := pol.UntilNext(); v >= (time.Millisecond * 100) {
		t.Errorf("Wrong period: %v", v)
	}

	if v := pol.Flush(); v == nil {
		t.Error("Nil msgs from flush")
	}

	if v := pol.UntilNext(); v >= (time.Millisecond*300) || v < (time.Millisecond*100) {
		t.Errorf("Wrong period: %v", v)
	}
}

func TestPolicySize(t *testing.T) {
	conf := NewPolicyConfig()
	conf.ByteSize = 10

	pol, err := NewPolicy(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{[]byte("foo bar"), []byte("baz qux")}

	if pol.Add(message.NewPart(exp[0])) {
		t.Error("Unexpected batch")
	}
	if !pol.Add(message.NewPart(exp[1])) {
		t.Error("Expected batch")
	}

	msg := pol.Flush()
	if !reflect.DeepEqual(exp, message.GetAllBytes(msg)) {
		t.Errorf("Wrong result: %s != %s", message.GetAllBytes(msg), exp)
	}

	if msg = pol.Flush(); msg != nil {
		t.Error("Non-nil empty flush")
	}
}

func TestPolicyCondition(t *testing.T) {
	cond := condition.NewConfig()
	cond.Type = condition.TypeText
	cond.Text.Operator = "equals"
	cond.Text.Arg = "bar"

	conf := NewPolicyConfig()
	conf.Condition = cond

	pol, err := NewPolicy(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := [][]byte{[]byte("foo"), []byte("bar")}

	if pol.Add(message.NewPart(exp[0])) {
		t.Error("Unexpected batch")
	}
	if !pol.Add(message.NewPart(exp[1])) {
		t.Error("Expected batch")
	}

	msg := pol.Flush()
	if !reflect.DeepEqual(exp, message.GetAllBytes(msg)) {
		t.Errorf("Wrong result: %s != %s", message.GetAllBytes(msg), exp)
	}

	if msg = pol.Flush(); msg != nil {
		t.Error("Non-nil empty flush")
	}
}
