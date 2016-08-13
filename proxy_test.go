package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var schema = fmt.Sprintf(`
{
	"namespace": "com.mediamath.changes",
	"type": "record",
	"name": "kafka_proxy_test_%v",
	"doc": "test the kafka proxy code",
	"fields": [
		{ "name": "name", "type":"string"}
	]
}`, time.Now().Unix())

type teststruct struct {
	Name string `json:"name"`
}

func tstClient() HTTPClient {
	return http.DefaultClient
}

func TestBackForthTopicFunctional(t *testing.T) {
	url := GetFunctionalTestURL(t)
	topic := GetFunctionalTestTopic(t)

	records := []*ProducerRecord{}
	for _, name := range []string{"rec1", "rec2", "rec3", "rec4", "rec5"} {
		b, err := json.Marshal(&teststruct{name})
		if err != nil {
			t.Fatalf("%s:%v", name, err)
		}

		records = append(records, &ProducerRecord{Value: json.RawMessage(b)})
	}

	msg1 := &ProducerMessage{
		ValueSchema: schema,
		Records:     records[:1],
	}

	resp, err := Produce(tstClient(), url, topic, msg1, Avro)
	require.Nil(t, err, fmt.Sprintf("%v", err))

	_, err = Consume(tstClient(), url, topic, resp.Offsets[0].Partition, resp.Offsets[0].Offset, 2, Avro)
	require.NoError(t, err)
}

func TestBackForthFunctional(t *testing.T) {
	url := GetFunctionalTestURL(t)
	topic := GetFunctionalTestTopic(t)

	records := []*ProducerRecord{}
	for _, name := range []string{"rec1", "rec2", "rec3", "rec4", "rec5"} {
		b, err := json.Marshal(&teststruct{name})
		if err != nil {
			t.Fatalf("%s:%v", name, err)
		}

		records = append(records, &ProducerRecord{Value: json.RawMessage(b)})
	}

	msg1 := &ProducerMessage{
		ValueSchema: schema,
		Records:     records[:1],
	}

	resp, err := Produce(tstClient(), url, topic, msg1, Avro)
	require.NoError(t, err)

	consumer, err := CreateConsumer(tstClient(), url, "proxy_test", NewConsumerRequest(Avro, Smallest))
	require.NoError(t, err)
	require.NotNil(t, consumer)

	_, err = WaitFor(tstClient(), consumer, topic, Avro, 2, 30*time.Second)
	if assert.NoError(t, err) {
		msg2 := &ProducerMessage{
			ValueSchemaID: resp.ValueSchemaID,
			Records:       records[1:],
		}

		_, err = Produce(tstClient(), url, topic, msg2, Avro)
		if assert.NoError(t, err) {

			_, err = WaitFor(tstClient(), consumer, topic, Avro, 3, 30*time.Second)
			assert.NoError(t, err)

			err = DeleteConsumerEndpoint(tstClient(), consumer)
			assert.NoError(t, err)

			_, err = ConsumeEndpoint(tstClient(), consumer, topic, Avro)
			assert.Error(t, err)
		}
	}

	DeleteConsumerEndpoint(tstClient(), consumer)
}
