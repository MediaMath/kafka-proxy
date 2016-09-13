package proxy

//Copyright 2016 MediaMath <http://www.mediamath.com>.  All rights reserved.
//Use of this source code is governed by a BSD-style
//license that can be found in the LICENSE file.

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	//TestURLEnvVar is the url to run functional tests against
	TestURLEnvVar = "KAFKA_PROXY_TEST_URL"
	//TestTopicEnvVar is the topic to run functional tests against
	TestTopicEnvVar = "KAFKA_PROXY_TEST_TOPIC"
	//TestRequiredEnvVar if set to true will make tests fail
	TestRequiredEnvVar = "KAFKA_PROXY_TEST_REQUIRED"
)

//IsFunctionalTestRequired returns whether SR_TEST_REQUIRED is set
func IsFunctionalTestRequired() bool {
	return strings.TrimSpace(os.Getenv(TestRequiredEnvVar)) == "true"
}

//HandleFunctionalTestError will skip or fail based on whether SR_TEST_REQUIRED is set
func HandleFunctionalTestError(t testing.TB, err error) {
	if err != nil && IsFunctionalTestRequired() {
		require.FailNow(t, err.Error())
	} else if err != nil {
		t.Skip(err)
	}
}

//GetFunctionalTestURL skips, fails, or returns the config variable passed in
func GetFunctionalTestURL(t *testing.T) string {
	if testing.Short() {
		t.Skipf("Skipping %v tests in short mode", TestURLEnvVar)
	}

	value := strings.TrimSpace(os.Getenv(TestURLEnvVar))

	if value == "" {
		HandleFunctionalTestError(t, fmt.Errorf("%v is undefined", TestURLEnvVar))
	}

	return value
}

//GetFunctionalTestTopic returns the configured test topic
func GetFunctionalTestTopic(t *testing.T) string {
	if testing.Short() {
		t.Skipf("Skipping %v tests in short mode", TestTopicEnvVar)
	}

	value := strings.TrimSpace(os.Getenv(TestTopicEnvVar))

	if value == "" {
		HandleFunctionalTestError(t, fmt.Errorf("%v is undefined", TestTopicEnvVar))
	}

	return value
}
