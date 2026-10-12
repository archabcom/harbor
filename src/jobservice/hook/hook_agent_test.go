// Copyright Project Harbor Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hook

import (
	"context"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/goharbor/harbor/src/jobservice/common/list"
	"github.com/goharbor/harbor/src/jobservice/common/utils"
	"github.com/goharbor/harbor/src/jobservice/errs"
	"github.com/goharbor/harbor/src/jobservice/job"
	"github.com/goharbor/harbor/src/jobservice/tests"
	"github.com/goharbor/harbor/src/lib/errors"
)

// HookAgentTestSuite tests functions of hook agent
type HookAgentTestSuite struct {
	suite.Suite

	namespace string
	pool      *redis.Pool
	agent     *basicAgent

	event *Event
	jid   string
}

// TestHookAgentTestSuite is entry of go test
func TestHookAgentTestSuite(t *testing.T) {
	suite.Run(t, new(HookAgentTestSuite))
}

// SetupSuite prepares test suites
func (suite *HookAgentTestSuite) SetupSuite() {
	suite.pool = tests.GiveMeRedisPool()
	suite.namespace = tests.GiveMeTestNamespace()

	suite.agent = &basicAgent{
		context:   context.TODO(),
		namespace: suite.namespace,
		redisPool: suite.pool,
	}

	suite.prepareData()
}

// TearDownSuite prepares test suites
func (suite *HookAgentTestSuite) TearDownSuite() {
	conn := suite.pool.Get()
	defer func() {
		_ = conn.Close()
	}()

	_ = tests.ClearAll(suite.namespace, conn)
}

func (suite *HookAgentTestSuite) prepareData() {
	suite.event = suite.newEvent()
	suite.jid = suite.event.Data.JobID
}

// newEvent saves the stats of a new running job and returns the event of its success.
func (suite *HookAgentTestSuite) newEvent() *Event {
	jid := utils.MakeIdentifier()
	rev := time.Now().Unix()
	stats := &job.Stats{
		Info: &job.StatsInfo{
			JobID:    jid,
			Status:   job.RunningStatus.String(),
			Revision: rev,
			JobKind:  job.KindGeneric,
			JobName:  job.SampleJob,
		},
	}
	t := job.NewBasicTrackerWithStats(context.TODO(), stats, suite.namespace, suite.pool, nil, list.New())
	err := t.Save()
	suite.NoError(err, "mock job stats")

	return &Event{
		URL:       "http://domian.com",
		Message:   "HookAgentTestSuite",
		Timestamp: time.Now().Unix(),
		Data: &job.StatusChange{
			JobID:  jid,
			Status: job.SuccessStatus.String(),
			Metadata: &job.StatsInfo{
				JobID:      jid,
				Status:     job.SuccessStatus.String(),
				Revision:   rev,
				UpdateTime: time.Now().Unix(),
				JobKind:    job.KindGeneric,
				JobName:    job.SampleJob,
			},
		},
	}
}

// TestEventSending ...
func (suite *HookAgentTestSuite) TestEventSending() {
	mc := &mockClient{}
	mc.On("SendEvent", suite.event).Return(nil)
	suite.agent.client = mc

	err := suite.agent.Trigger(suite.event)
	require.Nil(suite.T(), err, "agent trigger: nil error expected but got %s", err)

	// check
	suite.checkStatus()
}

// TestEventSending ...
func (suite *HookAgentTestSuite) TestEventSendingError() {
	mc := &mockClient{}
	mc.On("SendEvent", suite.event).Return(errors.New("internal server error: for testing"))
	suite.agent.client = mc

	err := suite.agent.Trigger(suite.event)

	suite.Error(err)
}

// TestEventRetryAck checks that an event delivered by a retry is ACKed, as when
// it is delivered directly, so the reaper does not send it again.
func (suite *HookAgentTestSuite) TestEventRetryAck() {
	evt := suite.newEvent()
	mc := &mockClient{}
	mc.On("SendEvent", evt).Return(errors.New("internal server error: for testing")).Once()
	mc.On("SendEvent", evt).Return(nil)

	agent := &basicAgent{
		context:   context.TODO(),
		namespace: suite.namespace,
		client:    mc,
		redisPool: suite.pool,
		tokens:    make(chan struct{}, 1),
	}
	suite.Error(agent.Trigger(evt))

	suite.Eventually(func() bool {
		t := job.NewBasicTrackerWithID(context.TODO(), evt.Data.JobID, suite.namespace, suite.pool, nil, list.New())
		if err := t.Load(); err != nil {
			return false
		}
		ack := t.Job().Info.HookAck
		return ack != nil && ack.Status == job.SuccessStatus.String()
	}, 10*time.Second, 100*time.Millisecond, "event delivered by a retry should be ACKed")
	mc.AssertNumberOfCalls(suite.T(), "SendEvent", 2)
}

// TestFreshNotFoundIsRetried checks the commit race: a 404 for a status change
// that just happened is still retried, and a later success is ACKed.
func (suite *HookAgentTestSuite) TestFreshNotFoundIsRetried() {
	evt := suite.newEvent()
	mc := &mockClient{}
	mc.On("SendEvent", evt).Return(errs.NoObjectFoundError(evt.Data.JobID)).Once()
	mc.On("SendEvent", evt).Return(nil)

	agent := &basicAgent{
		context:   context.TODO(),
		namespace: suite.namespace,
		client:    mc,
		redisPool: suite.pool,
		tokens:    make(chan struct{}, 1),
	}
	suite.Error(agent.Trigger(evt))

	suite.Eventually(func() bool {
		t := job.NewBasicTrackerWithID(context.TODO(), evt.Data.JobID, suite.namespace, suite.pool, nil, list.New())
		if err := t.Load(); err != nil {
			return false
		}
		ack := t.Job().Info.HookAck
		return ack != nil && ack.Status == job.SuccessStatus.String()
	}, 10*time.Second, 100*time.Millisecond, "fresh not-found hook should be retried and then ACKed")
	mc.AssertNumberOfCalls(suite.T(), "SendEvent", 2)
}

// TestRetryStopsWhenTargetDisappears checks an in-flight retry: the first send
// failed for a transient reason, and the retry then learns the task is gone.
// The backoff must stop instead of running out the full window.
func (suite *HookAgentTestSuite) TestRetryStopsWhenTargetDisappears() {
	evt := suite.newEvent()
	stale := time.Now().Add(-errRetryBackoff - time.Minute).Unix()
	evt.Timestamp = stale
	evt.Data.Metadata.UpdateTime = stale

	mc := &mockClient{}
	mc.On("SendEvent", evt).Return(errors.New("timeout")).Once()
	mc.On("SendEvent", evt).Return(errs.NoObjectFoundError(evt.Data.JobID))

	agent := &basicAgent{
		context:   context.TODO(),
		namespace: suite.namespace,
		client:    mc,
		redisPool: suite.pool,
		tokens:    make(chan struct{}, 1),
	}
	suite.Error(agent.Trigger(evt))

	// Returning the not-found error would schedule another attempt after the
	// initial backoff interval (about 1s to 3s). Waiting past that shows the
	// retry stopped.
	time.Sleep(4 * time.Second)
	mc.AssertNumberOfCalls(suite.T(), "SendEvent", 2)

	t := job.NewBasicTrackerWithID(context.TODO(), evt.Data.JobID, suite.namespace, suite.pool, nil, list.New())
	suite.NoError(t.Load())
	suite.Nil(t.Job().Info.HookAck, "an undeliverable hook must not be ACKed")
}

// TestStaleNotFoundIsNotRetried checks that a 404 for a long-gone task does not
// enter the backoff loop that floods jobservice logs.
func (suite *HookAgentTestSuite) TestStaleNotFoundIsNotRetried() {
	evt := suite.newEvent()
	stale := time.Now().Add(-errRetryBackoff - time.Minute).Unix()
	evt.Timestamp = stale
	evt.Data.Metadata.UpdateTime = stale

	mc := &mockClient{}
	mc.On("SendEvent", evt).Return(errs.NoObjectFoundError(evt.Data.JobID))

	agent := &basicAgent{
		context:   context.TODO(),
		namespace: suite.namespace,
		client:    mc,
		redisPool: suite.pool,
		tokens:    make(chan struct{}, 1),
	}
	suite.Error(agent.Trigger(evt))

	// The backoff's first retry is immediate. A second call inside this window
	// would mean the abandon path failed to skip the retry goroutine.
	time.Sleep(time.Second)
	mc.AssertNumberOfCalls(suite.T(), "SendEvent", 1)

	t := job.NewBasicTrackerWithID(context.TODO(), evt.Data.JobID, suite.namespace, suite.pool, nil, list.New())
	suite.NoError(t.Load())
	suite.Nil(t.Job().Info.HookAck, "an undeliverable hook must not be ACKed")
}

func TestAbandonRetry(t *testing.T) {
	notFound := errs.NoObjectFoundError("job")
	other := errors.New("boom")
	old := time.Now().Add(-errRetryBackoff - time.Minute).Unix()
	fresh := time.Now().Unix()

	cases := []struct {
		name string
		evt  *Event
		err  error
		want bool
	}{
		{name: "nil error", evt: &Event{Timestamp: old}, err: nil, want: false},
		{name: "other error", evt: &Event{Timestamp: old}, err: other, want: false},
		{name: "fresh not found", evt: &Event{Timestamp: fresh}, err: notFound, want: false},
		{name: "old not found via timestamp", evt: &Event{Timestamp: old}, err: notFound, want: true},
		{
			name: "old not found via metadata",
			evt:  &Event{Data: &job.StatusChange{Metadata: &job.StatsInfo{UpdateTime: old}}},
			err:  notFound,
			want: true,
		},
		{
			name: "metadata update time wins over a fresh timestamp",
			evt: &Event{
				Timestamp: fresh,
				Data:      &job.StatusChange{Metadata: &job.StatsInfo{UpdateTime: old}},
			},
			err:  notFound,
			want: true,
		},
		{name: "not found with unknown time", evt: &Event{}, err: notFound, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, abandonRetry(tc.evt, tc.err))
		})
	}
}

func (suite *HookAgentTestSuite) checkStatus() {
	t := job.NewBasicTrackerWithID(context.TODO(), suite.jid, suite.namespace, suite.pool, nil, list.New())
	err := t.Load()
	require.NoError(suite.T(), err, "load updated job stats")
	require.NotNil(suite.T(), t.Job(), "latest job stats")
	suite.Equal(job.SuccessStatus.String(), t.Job().Info.HookAck.Status, "ack status")
}

type mockClient struct {
	mock.Mock
}

func (mc *mockClient) SendEvent(evt *Event) error {
	args := mc.Called(evt)
	return args.Error(0)
}
