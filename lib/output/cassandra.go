// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package output

import (
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/output/writer"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeFiles] = TypeSpec{
		constructor: NewCassandra,
		description: `
Writes each individual part of each message to cassandra table, batched 
by message.

Messages will be inserted as one cassandra batch per message, with each 
Part (in JSON format) becoming one INSERT statement within that Batch. 
When the message is complete, the Batch is committed. If any member of 
the Batch fails, the entire Batch will fail.

Schema is detirmined by the "field_mappings" configuration parameter. 
More on jsonpath can be found (here)[https://www.pluralsight.com/blog/
tutorials/introduction-to-jsonpath]. 

exmple:
`+"```"+`
type: cassandra
cassandra:
  field_mappings:
    field1: "$.path.to.field.in.part"
    field2: "$.a.different.path"
`+"```",
	}
}

//------------------------------------------------------------------------------

// NewCassandra creates a new Cassandra output type.
func NewCassandra(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	c,err := writer.NewCassandra(config.(*CassandraConfig), logger, metrics)
	if err != nil {
		return nil, err
	}
	return output.NewWriter("cassandra", c, logger, metrics)
}

//------------------------------------------------------------------------------
