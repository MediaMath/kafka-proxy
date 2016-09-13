package proxy

//Copyright 2016 MediaMath <http://www.mediamath.com>.  All rights reserved.
//Use of this source code is governed by a BSD-style
//license that can be found in the LICENSE file.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"time"
)

type (
	//HTTPClient is any client that can do a http request
	HTTPClient interface {
		Do(request *http.Request) (*http.Response, error)
	}

	//Message is a single kafka message
	Message struct {
		Key       json.RawMessage `json:"key"`
		Value     json.RawMessage `json:"value"`
		Partition int32           `json:"partition"`
		Offset    int64           `json:"offset"`
	}

	//ConsumerRequest is the meta information needed to create a consumer
	ConsumerRequest struct {
		Format     Format `json:"format"`
		Offset     Offset `json:"auto.offset.reset"`
		AutoCommit string `json:"auto.commit.enable"`
		Name       string `json:"name,omitempty"`
	}

	//ConsumerEndpoint is returned on a create
	ConsumerEndpoint struct {
		InstanceID string `json:"instance_id"`
		BaseURI    string `json:"base_uri"`
	}

	//Format is one of json, binary or avro
	Format string

	//Offset is either smallest or largest
	Offset string
)

const (
	//JSON formated consumer
	JSON = Format("json")
	//Binary formated consumer
	Binary = Format("binary")
	//Avro formated consumer
	Avro = Format("avro")

	//Smallest is the offset that is oldest
	Smallest = "smallest"
	//Largest is the offset that is newest
	Largest = "largest"
)

//Consume takes a kafka location and consumes messages for it
func Consume(client HTTPClient, baseURL string, topic string, partition int32, offset int64, count int, format Format) ([]*Message, error) {
	if count < 1 {
		return nil, fmt.Errorf("Count must be 1 or greater: %v", count)
	}

	req, err := ConsumeRequest(baseURL, topic, partition, offset, count, format)
	if err != nil {
		return nil, err
	}

	resp := []*Message{}
	status, body, err := doJSON(client, req, &resp)

	if status != http.StatusOK {
		return nil, fmt.Errorf("%v:%s", status, body)
	}

	if err != nil {
		return nil, fmt.Errorf("%v:%s:%v", status, body, err)
	}

	return resp, nil
}

//ConsumeRequest builds the request for the /topics/<topic>/partitions/<partition>/messages route
func ConsumeRequest(baseURL string, topic string, partition int32, offset int64, count int, format Format) (*http.Request, error) {
	contentType, err := contentTypeForFormat(format)
	if err != nil {
		return nil, err
	}

	countQuery := url.QueryEscape(fmt.Sprintf("%v", count))
	offsetQuery := url.QueryEscape(fmt.Sprintf("%v", offset))

	return get(
		baseURL,
		path.Join("topics", topic, "partitions", fmt.Sprintf("%v", partition), "messages"),
		fmt.Sprintf("offset=%s&count=%s", offsetQuery, countQuery),
		contentType,
	)
}

//NewConsumerRequest creates a consumer request for the provided format and offset
func NewConsumerRequest(format Format, offset Offset) *ConsumerRequest {
	return &ConsumerRequest{
		Format:     format,
		Offset:     offset,
		AutoCommit: "true",
	}
}

//CreateConsumer will create a consumer on the proxy for later use
func CreateConsumer(client HTTPClient, baseURL string, group string, request *ConsumerRequest) (resp *ConsumerEndpoint, err error) {
	var req *http.Request
	req, err = CreateConsumerRequest(baseURL, group, request)
	if err != nil {
		return
	}

	resp = &ConsumerEndpoint{}
	var status int
	var body []byte
	status, body, err = doJSON(client, req, resp)
	if err != nil {
		return
	}

	if status != http.StatusOK {
		err = fmt.Errorf("%v:%s", status, body)
		resp = nil
		return
	}

	return
}

//CreateConsumerRequest will create a request for the /consumers/<group> endpoint
func CreateConsumerRequest(baseURL string, group string, request *ConsumerRequest) (*http.Request, error) {
	return post(baseURL, path.Join("consumers", group), request, "application/vnd.kafka.v1+json")
}

//ConsumeEndpoint will get the next messages off of the previously created consumer endpoint. Format must match the previously created format
func ConsumeEndpoint(client HTTPClient, endpoint *ConsumerEndpoint, topic string, format Format) ([]*Message, error) {
	req, err := ConsumeEndpointRequest(endpoint, topic, format)
	if err != nil {
		return nil, err
	}

	resp := []*Message{}
	status, body, err := doJSON(client, req, &resp)
	if err != nil {
		return nil, fmt.Errorf("%v:%s:%v", status, body, err)
	}

	if status != http.StatusOK {
		return nil, fmt.Errorf("%v:%s", status, body)
	}

	return resp, nil
}

//ConsumeEndpointRequest will create a request for consumerURL/topics/<topic>
func ConsumeEndpointRequest(consumer *ConsumerEndpoint, topic string, format Format) (*http.Request, error) {
	contentType, err := contentTypeForFormat(format)
	if err != nil {
		return nil, err
	}

	return get(consumer.BaseURI, path.Join("topics", topic), "", contentType)
}

//DeleteConsumerEndpoint will delete a previously created consumer endpoint
func DeleteConsumerEndpoint(client HTTPClient, endpoint *ConsumerEndpoint) error {
	req, err := http.NewRequest("DELETE", endpoint.BaseURI, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	status := resp.StatusCode

	if err != nil {
		return err
	}

	if status != http.StatusNoContent {
		return fmt.Errorf("%v:%s", status, body)
	}

	return nil
}

//WaitFor will return all the messages up to the timeout or count
func WaitFor(client HTTPClient, endpoint *ConsumerEndpoint, topic string, format Format, count int, timeout time.Duration) ([]*Message, error) {
	messages, err := ConsumeEndpoint(client, endpoint, topic, format)
	if err != nil {
		return nil, err
	}

	timedout := make(chan error)
	go func() {
		<-time.After(timeout)
		close(timedout)
	}()

	for len(messages) < count {
		select {
		case <-timedout:
			return nil, fmt.Errorf("Timed out: %v", len(messages))
		default:
			<-time.After(500 * time.Millisecond)
		}

		more, err := ConsumeEndpoint(client, endpoint, topic, format)
		if err != nil {
			return nil, err
		}

		messages = append(messages, more...)
	}

	return messages, nil
}

func doJSON(restful HTTPClient, request *http.Request, response interface{}) (status int, body []byte, err error) {
	res, err := restful.Do(request)
	if err == nil {
		body, err = ioutil.ReadAll(res.Body)
		res.Body.Close()
		status = res.StatusCode
	}

	if err == nil && response != nil {
		err = json.Unmarshal(body, response)
	}

	return
}

func get(baseURL, path, query, accepts string) (request *http.Request, err error) {
	var u string
	u, err = buildURL(baseURL, path, query)
	if err != nil {
		return
	}

	request, err = http.NewRequest("GET", u, nil)
	if request != nil {
		request.Header.Add("Accept", accepts)
	}

	return
}

func post(baseURL, path string, body interface{}, contentType string) (request *http.Request, err error) {
	var reader io.Reader
	if body != nil {
		var data []byte
		data, err = json.Marshal(body)
		if err != nil {
			return
		}
		reader = bytes.NewBuffer(data)
	}

	var u string
	u, err = buildURL(baseURL, path, "")
	if err != nil {
		return
	}

	request, err = http.NewRequest("POST", u, reader)
	if request != nil {
		request.Header.Add("Content-Type", contentType)
	}

	return
}

func contentTypeForFormat(format Format) (contentType string, err error) {
	switch format {
	case JSON:
		contentType = "application/vnd.kafka.json.v1+json"
	case Binary:
		contentType = "application/vnd.kafka.binary.v1+json"
	case Avro:
		contentType = "application/vnd.kafka.avro.v1+json"
	default:
		err = fmt.Errorf("Unknown format: %v", format)
	}

	return
}

func buildURL(baseURL, p string, query string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	u.Path = path.Join(u.Path, p)

	if query != "" {
		u.RawQuery = query
	}

	return u.String(), nil
}
