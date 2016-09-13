package proxy

//Copyright 2016 MediaMath <http://www.mediamath.com>.  All rights reserved.
//Use of this source code is governed by a BSD-style
//license that can be found in the LICENSE file.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
)

//ProducerResponse is the response the kafka rest proxy returns on message production
type ProducerResponse struct {
	KeySchemaID   int                `json:"key_schema_id"`
	ValueSchemaID int                `json:"value_schema_id"`
	Offsets       []*ProducerOffsets `json:"offsets"`
}

//ProducerOffsets are the resulting offsets for a produced set of messages
type ProducerOffsets struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	ErrorCode int64  `json:"error_code"`
	Error     string `json:"error"`
}

//ProducerMessage is the wrapper for the data to the kafka rest proxy producer endpoint
type ProducerMessage struct {
	KeySchema     string            `json:"key_schema,omitempty"`
	KeySchemaID   int               `json:"key_schema_id,omitempty"`
	ValueSchema   string            `json:"value_schema,omitempty"` //either value schema or value schema id must be provided for avro messages
	ValueSchemaID int               `json:"value_schema_id,omitempty"`
	Records       []*ProducerRecord `json:"records"`
}

//ProducerRecord is an individual message to be produced on kafka
type ProducerRecord struct {
	Key       json.RawMessage `json:"key,omitempty"`
	Value     json.RawMessage `json:"value"`
	Partition int32           `json:"partition,omitempty"`
}

//Produce will publish the message to the topic
func Produce(client HTTPClient, baseURL string, topic string, message *ProducerMessage, format Format) (*ProducerResponse, error) {
	if format == Avro && message.ValueSchema == "" && message.ValueSchemaID == 0 {
		return nil, fmt.Errorf("Must provide a value schema or value schema id")
	}

	req, err := ProduceRequest(baseURL, topic, format, message)
	if err != nil {
		return nil, err
	}

	resp := &ProducerResponse{}
	status, body, err := doJSON(client, req, resp)
	if err != nil {
		return nil, err
	}

	if status != http.StatusOK {
		return nil, fmt.Errorf("%v:%s", status, body)
	}

	return resp, nil
}

//ProduceRequest creates the request for POST /topics/<topic> endpoint
func ProduceRequest(baseURL string, topic string, format Format, message *ProducerMessage) (*http.Request, error) {
	contentType, err := contentTypeForFormat(format)
	if err != nil {
		return nil, err
	}

	return post(baseURL, path.Join("topics", topic), message, contentType)
}
