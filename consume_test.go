package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp"
	"github.com/heetch/avro"
	"github.com/heetch/avro/avroregistry"
	"gopkg.in/retry.v1"
)

func TestParseOffsets(t *testing.T) {
	data := []struct {
		testName    string
		input       string
		expected    map[int32]interval
		expectedErr string
	}{{
		testName: "empty",
		input:    "",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "single-comma",
		input:    ",",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "all",
		input:    "all",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "oldest",
		input:    "oldest",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "all-with-space",
		input: "	all ",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "all-with-zero-initial-offset",
		input:    "all=+0:",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "several-partitions",
		input:    "1,2,4",
		expected: map[int32]interval{
			1: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
			2: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
			4: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "one-partition,empty-offsets",
		input:    "0=",
		expected: map[int32]interval{
			0: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "one-partition,one-offset",
		input:    "0=1",
		expected: map[int32]interval{
			0: interval{
				start: positionAtOffset(1),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "one-partition,empty-after-colon",
		input:    "0=1:",
		expected: map[int32]interval{
			0: interval{
				start: positionAtOffset(1),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "multiple-partitions",
		input:    "0=4:,2=1:10,6",
		expected: map[int32]interval{
			0: interval{
				start: positionAtOffset(4),
				end:   positionAtOffset(maxOffset),
			},
			2: interval{
				start: positionAtOffset(1),
				end:   positionAtOffset(10),
			},
			6: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "newest-relative",
		input:    "0=-1",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetNewest),
					diff:   anchorDiff{offset: -1},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "newest-relative,empty-after-colon",
		input:    "0=-1:",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetNewest),
					diff:   anchorDiff{offset: -1},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "oldest-relative",
		input:    "0=+1",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetOldest),
					diff:   anchorDiff{offset: 1},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "oldest-relative,empty-after-colon",
		input:    "0=+1:",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetOldest),
					diff:   anchorDiff{offset: 1},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "oldest-relative-to-newest-relative",
		input:    "0=+1:-1",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetOldest),
					diff:   anchorDiff{offset: 1},
				},
				end: position{
					anchor: anchorAtOffset(sarama.OffsetNewest),
					diff:   anchorDiff{offset: -1},
				},
			},
		},
	}, {
		testName: "specific-partition-with-all-partitions",
		input:    "0=+1:-1,all=1:10",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetOldest),
					diff:   anchorDiff{offset: 1},
				},
				end: position{
					anchor: anchorAtOffset(sarama.OffsetNewest),
					diff:   anchorDiff{offset: -1},
				},
			},
			-1: interval{
				start: positionAtOffset(1),
				end:   positionAtOffset(10),
			},
		},
	}, {
		testName: "oldest-to-newest",
		input:    "0=oldest:newest",
		expected: map[int32]interval{
			0: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(sarama.OffsetNewest),
			},
		},
	}, {
		testName: "oldest-to-newest-with-offsets",
		input:    "0=oldest+10:newest-10",
		expected: map[int32]interval{
			0: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetOldest),
					diff:   anchorDiff{offset: 10},
				},
				end: position{
					anchor: anchorAtOffset(sarama.OffsetNewest),
					diff:   anchorDiff{offset: -10},
				},
			},
		},
	}, {
		testName: "newest",
		input:    "newest",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetNewest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "single-partition",
		input:    "10",
		expected: map[int32]interval{
			10: interval{
				start: positionAtOffset(sarama.OffsetOldest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "single-range,all-partitions",
		input:    "10:20",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(10),
				end:   positionAtOffset(20),
			},
		},
	}, {
		testName: "single-range,all-partitions,open-end",
		input:    "10:",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(10),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "all-newest",
		input:    "all=newest:",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(sarama.OffsetNewest),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "implicit-all-newest-with-offset",
		input:    "newest-10:",
		expected: map[int32]interval{
			-1: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetNewest),
					diff:   anchorDiff{offset: -10},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "implicit-all-oldest-with-offset",
		input:    "oldest+10:",
		expected: map[int32]interval{
			-1: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetOldest),
					diff:   anchorDiff{offset: 10},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "implicit-all-neg-offset-empty-colon",
		input:    "-10:",
		expected: map[int32]interval{
			-1: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetNewest),
					diff:   anchorDiff{offset: -10},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "implicit-all-pos-offset-empty-colon",
		input:    "+10:",
		expected: map[int32]interval{
			-1: interval{
				start: position{
					anchor: anchorAtOffset(sarama.OffsetOldest),
					diff:   anchorDiff{offset: 10},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "start-offset-combines-with-diff-offset",
		input:    "1000+3",
		expected: map[int32]interval{
			-1: interval{
				start: positionAtOffset(1003),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName:    "invalid-partition",
		input:       "bogus",
		expectedErr: `invalid anchor position "bogus"`,
	}, {
		testName:    "several-colons",
		input:       ":::",
		expectedErr: `invalid position ":::"`,
	}, {
		testName:    "bad-relative-offset-start",
		input:       "foo+20",
		expectedErr: `invalid anchor position "foo"`,
	}, {
		testName:    "bad-relative-offset-diff",
		input:       "oldest+bad",
		expectedErr: `invalid relative position "\+bad"`,
	}, {
		testName:    "bad-relative-offset-diff-at-start",
		input:       "+bad",
		expectedErr: `invalid relative position "\+bad"`,
	}, {
		testName:    "relative-offset-too-big",
		input:       "+9223372036854775808",
		expectedErr: `offset "\+9223372036854775808" is too large`,
	}, {
		testName:    "starting-offset-too-big",
		input:       "9223372036854775808:newest",
		expectedErr: `anchor offset "9223372036854775808" is too large`,
	}, {
		testName:    "ending-offset-too-big",
		input:       "oldest:9223372036854775808",
		expectedErr: `anchor offset "9223372036854775808" is too large`,
	}, {
		testName:    "partition-too-big",
		input:       "2147483648=oldest",
		expectedErr: `partition number "2147483648" is too large`,
	}, {
		testName: "time-anchor-rfc3339",
		input:    "[2019-08-31T13:06:08.234Z]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-08-31T13:06:08.234Z")),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "time-anchor-rfc3339-not-utc",
		input:    "[2019-08-31T13:06:08.234-04:00]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-08-31T17:06:08.234Z")),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "time-anchor-date",
		input:    "[2019-08-31]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-08-31T00:00:00Z")),
				end:   positionAtTime(T("2019-09-01T00:00:00Z")),
			},
		},
	}, {
		testName: "time-anchor-imprecise-explicit-colon",
		input:    "[2019-08-31]:",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-08-31T00:00:00Z")),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "time-anchor-date-explicit-end",
		input:    "[2019-08-31]:[2019-09-04]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-08-31T00:00:00Z")),
				end:   positionAtTime(T("2019-09-05T00:00:00Z")),
			},
		},
	}, {
		testName: "time-anchor-month",
		input:    "[2019-08]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-08-01T00:00:00Z")),
				end:   positionAtTime(T("2019-09-01T00:00:00Z")),
			},
		},
	}, {
		testName: "time-anchor-year",
		input:    "[2019]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-01-01T00:00:00Z")),
				end:   positionAtTime(T("2020-01-01T00:00:00Z")),
			},
		},
	}, {
		testName: "time-anchor-minute",
		input:    "[13:45]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2011-02-03T13:45:00Z")),
				end:   positionAtTime(T("2011-02-03T13:46:00Z")),
			},
		},
	}, {
		testName: "time-anchor-second",
		input:    "[13:45:12.345]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2011-02-03T13:45:12.345Z")),
				end:   positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "time-anchor-hour",
		input:    "[4pm]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2011-02-03T16:00:00Z")),
				end:   positionAtTime(T("2011-02-03T17:00:00Z")),
			},
		},
	}, {
		testName: "time-range",
		input:    "[2019-08-31T13:06:08.234Z]:[2023-02-05T12:01:02.6789Z]",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2019-08-31T13:06:08.234Z")),
				end:   positionAtTime(T("2023-02-05T12:01:02.6789Z")),
			},
		},
	}, {
		testName: "time-anchor-with-diff-offset",
		input:    "[4pm]-123",
		expected: map[int32]interval{
			-1: {
				start: position{
					anchor: anchorAtTime(T("2011-02-03T16:00:00Z")),
					diff:   anchorDiff{offset: -123},
				},
				end: position{
					anchor: anchorAtTime(T("2011-02-03T17:00:00Z")),
					diff:   anchorDiff{offset: -123},
				},
			},
		},
	}, {
		testName: "offset-anchor-with-negative-time-rel",
		input:    "1234-1h3s",
		expected: map[int32]interval{
			-1: {
				start: position{
					anchor: anchorAtOffset(1234),
					diff: anchorDiff{
						isDuration: true,
						duration:   -(time.Hour + 3*time.Second),
					},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "offset-anchor-with-positive-time-rel",
		input:    "1234+555ms",
		expected: map[int32]interval{
			-1: {
				start: position{
					anchor: anchorAtOffset(1234),
					diff: anchorDiff{
						isDuration: true,
						duration:   555 * time.Millisecond,
					},
				},
				end: positionAtOffset(maxOffset),
			},
		},
	}, {
		testName: "time-anchor-combined-with-time-rel",
		input:    "[3pm]+5s",
		expected: map[int32]interval{
			-1: {
				start: positionAtTime(T("2011-02-03T15:00:05Z")),
				end:   positionAtTime(T("2011-02-03T16:00:05Z")),
			},
		},
	},
	// TODO error cases
	// TODO local time resolution
	}
	c := qt.New(t)
	// Choose a reference date that's not UTC, so we can ensure
	// that the timezone-dependent logic works correctly.
	now := T("2011-02-03T16:05:06.500Z").In(time.FixedZone("UTC-8", -8*60*60))
	for _, d := range data {
		c.Run(d.testName, func(c *qt.C) {
			actual, err := parseOffsets(d.input, now)
			if d.expectedErr != "" {
				c.Assert(err, qt.ErrorMatches, d.expectedErr)
				return
			}
			c.Assert(err, qt.Equals, nil)
			c.Assert(actual, deepEquals, d.expected)
		})
	}
}

func BenchmarkMerge(b *testing.B) {
	c := qt.New(b)
	const npart = 1000
	nmsgs := b.N / npart
	cs := make([]<-chan *sarama.ConsumerMessage, npart)
	epoch := time.Date(2019, 10, 7, 12, 0, 0, 0, time.UTC)
	for i := range cs {
		c := make(chan *sarama.ConsumerMessage, 10)
		cs[i] = c
		go func() {
			defer close(c)
			t := epoch
			for i := 0; i < nmsgs; i++ {
				c <- &sarama.ConsumerMessage{
					Timestamp: t,
				}
				t = t.Add(time.Second)
			}
		}()
	}
	b.ResetTimer()
	total := 0
	for range mergeConsumers(cs...) {
		total++
	}
	c.Assert(total, qt.Equals, npart*nmsgs)
}

func BenchmarkNoMerge(b *testing.B) {
	const npart = 1000
	nmsgs := b.N / npart
	epoch := time.Date(2019, 10, 7, 12, 0, 0, 0, time.UTC)
	c := make(chan *sarama.ConsumerMessage, 10)
	for i := 0; i < npart; i++ {
		go func() {
			t := epoch
			for i := 0; i < nmsgs; i++ {
				c <- &sarama.ConsumerMessage{
					Timestamp: t,
				}
				t = t.Add(time.Second)
			}
		}()
	}
	b.ResetTimer()
	for i := 0; i < npart*nmsgs; i++ {
		<-c
	}
}

func TestMerge(t *testing.T) {
	c := qt.New(t)
	epoch := time.Date(2019, 10, 7, 12, 0, 0, 0, time.UTC)
	M := func(timestamp int) *sarama.ConsumerMessage {
		return &sarama.ConsumerMessage{
			Timestamp: epoch.Add(time.Duration(timestamp) * time.Hour),
		}
	}
	partitionMsgs := map[int32][]*sarama.ConsumerMessage{
		0: {M(0), M(2), M(3), M(10)},
		1: {M(1), M(4), M(5), M(6)},
		2: {M(7), M(8), M(9)},
		3: {M(11), M(12)},
		4: {},
	}
	var wantMsgs []*sarama.ConsumerMessage
	for p, msgs := range partitionMsgs {
		for i, m := range msgs {
			m.Partition = p
			m.Offset = int64(i)
		}
		wantMsgs = append(wantMsgs, msgs...)
	}
	sort.Slice(wantMsgs, func(i, j int) bool {
		return wantMsgs[i].Timestamp.Before(wantMsgs[j].Timestamp)
	})
	// Start a consumer
	chans := make([]<-chan *sarama.ConsumerMessage, 0, len(wantMsgs))
	for _, msgs := range partitionMsgs {
		msgs := msgs
		c := make(chan *sarama.ConsumerMessage)
		go func() {
			defer close(c)
			for _, m := range msgs {
				c <- m
				time.Sleep(time.Millisecond)
			}
		}()
		chans = append(chans, c)
	}
	resultc := mergeConsumers(chans...)
	var gotMsgs []*sarama.ConsumerMessage
loop:
	for {
		select {
		case m, ok := <-resultc:
			if !ok {
				break loop
			}
			gotMsgs = append(gotMsgs, m)
		case <-time.After(5 * time.Second):
			c.Fatal("timed out waiting for messages")
		}
	}
	c.Assert(gotMsgs, qt.HasLen, len(wantMsgs))
	c.Assert(gotMsgs, qt.DeepEquals, wantMsgs)
}

func TestConsume(t *testing.T) {
	c := qt.New(t)
	closer := make(chan struct{})
	messageChan := make(<-chan *sarama.ConsumerMessage)
	calls := make(chan tConsumePartition)
	consumer := tConsumer{
		consumePartition: map[tConsumePartition]tPartitionConsumer{
			tConsumePartition{"hans", 1, 1}: tPartitionConsumer{messages: messageChan},
			tConsumePartition{"hans", 2, 1}: tPartitionConsumer{messages: messageChan},
		},
		calls: calls,
	}
	target := consumeCmd{consumer: consumer}
	target.topic = "hans"
	target.brokerStrs = []string{"localhost:9092"}

	go target.consume(map[int32]resolvedInterval{
		1: {1, 5},
		2: {1, 5},
	}, map[int32]int64{
		1: 1,
		2: 1,
	})
	defer close(closer)

	var actual []tConsumePartition
	expected := []tConsumePartition{
		tConsumePartition{"hans", 1, 1},
		tConsumePartition{"hans", 2, 1},
	}
	timeout := time.After(time.Second)
	for {
		select {
		case call := <-calls:
			actual = append(actual, call)
			if len(actual) < len(expected) {
				break
			}
			sort.Sort(ByPartitionOffset(actual))
			c.Check(actual, qt.DeepEquals, expected)
			return
		case <-timeout:
			c.Fatalf("Did not receive calls to consume partitions before timeout.")
		}
	}
}

type tConsumePartition struct {
	Topic     string
	Partition int32
	Offset    int64
}

type ByPartitionOffset []tConsumePartition

func (a ByPartitionOffset) Len() int {
	return len(a)
}
func (a ByPartitionOffset) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a ByPartitionOffset) Less(i, j int) bool {
	if a[i].Partition != a[j].Partition {
		return a[i].Partition < a[j].Partition
	}
	return a[i].Offset < a[j].Offset
}

type tPartitionConsumer struct {
	closeErr            error
	highWaterMarkOffset int64
	messages            <-chan *sarama.ConsumerMessage
	errors              <-chan *sarama.ConsumerError
}

func (pc tPartitionConsumer) AsyncClose() {}

func (pc tPartitionConsumer) Close() error {
	return pc.closeErr
}

func (pc tPartitionConsumer) HighWaterMarkOffset() int64 {
	return pc.highWaterMarkOffset
}

func (pc tPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return pc.messages
}

func (pc tPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return pc.errors
}

type tConsumer struct {
	topics              []string
	topicsErr           error
	partitions          map[string][]int32
	partitionsErr       map[string]error
	consumePartition    map[tConsumePartition]tPartitionConsumer
	consumePartitionErr map[tConsumePartition]error
	closeErr            error
	calls               chan tConsumePartition
}

func (c tConsumer) Topics() ([]string, error) {
	return c.topics, c.topicsErr
}

func (c tConsumer) Partitions(topic string) ([]int32, error) {
	return c.partitions[topic], c.partitionsErr[topic]
}

func (c tConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	cp := tConsumePartition{topic, partition, offset}
	c.calls <- cp
	return c.consumePartition[cp], c.consumePartitionErr[cp]
}

func (c tConsumer) Close() error {
	return c.closeErr
}

func (c tConsumer) HighWaterMarks() map[string]map[int32]int64 {
	return nil
}

func TestConsumeParseArgsUsesEnvVar(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	registry := "localhost:8084"
	broker := "hans:2000"

	c.Setenv(ENV_BROKERS, broker)
	c.Setenv(ENV_REGISTRY, registry)

	cmd0, _, err := parseCmd("hkt", "consume")
	c.Assert(err, qt.Equals, nil)
	cmd := cmd0.(*consumeCmd)
	c.Assert(cmd.brokers(), qt.DeepEquals, []string{broker})
	c.Assert(cmd.registryURL, qt.Equals, registry)
}

// brokers default to localhost:9092
func TestConsumeParseArgsDefault(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	c.Setenv(ENV_BROKERS, "")
	c.Setenv(ENV_REGISTRY, "")

	cmd0, _, err := parseCmd("hkt", "consume")
	c.Assert(err, qt.Equals, nil)
	cmd := cmd0.(*consumeCmd)
	c.Assert(cmd.brokers(), qt.DeepEquals, []string{"localhost:9092"})
	c.Assert(cmd.registryURL, qt.Equals, "")
}

func TestConsumeParseArgsFlagsOverrideEnv(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	registry := "localhost:8084"
	broker := "hans:2000"

	// command line arg wins
	c.Setenv(ENV_BROKERS, "BLABB")
	c.Setenv(ENV_REGISTRY, "BLABB")

	cmd0, _, err := parseCmd("hkt", "consume", "-brokers", broker, "-registry", registry)
	c.Assert(err, qt.Equals, nil)
	cmd := cmd0.(*consumeCmd)
	c.Assert(cmd.brokers(), qt.DeepEquals, []string{broker})
	c.Assert(cmd.registryURL, qt.Equals, registry)
}

func TestConsumeAvroMessage(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	type record struct {
		A int
		B int
	}

	// In the byte slice below:
	//	80: A=40
	//	40: B=20
	rec := record{A: 40, B: 20}
	data := []byte{80, 40}

	_, wType, err := avro.Marshal(rec)
	c.Assert(err, qt.IsNil)

	reg := newTestRegistry(c)
	schemaID := reg.register(c, wType)

	cmd := consumeCmd{coder: coder{registry: reg.registry}}

	enc, err := cmd.encoderForType("key", "string")
	c.Assert(err, qt.IsNil)
	cmd.encodeKey = enc

	enc, err = cmd.encoderForType("value", "avro")
	c.Assert(err, qt.IsNil)
	cmd.encodeValue = enc

	msg := &sarama.ConsumerMessage{
		Key:       []byte("foo"),
		Value:     createAvroMessage(schemaID, data),
		Partition: 1,
		Offset:    0,
	}

	consumed, err := cmd.newConsumedMessage(msg)
	c.Assert(err, qt.IsNil)

	var got record
	err = json.Unmarshal(consumed.Value, &got)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, rec)
}

func T(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// deepEquals allows comparison of the unexported fields inside the
// struct types that we test internally.
var deepEquals = qt.CmpEquals(cmp.AllowUnexported(
	interval{},
	position{},
	anchor{},
	anchorDiff{},
	producerMessage{},
))

func positionAtOffset(off int64) position {
	return position{
		anchor: anchorAtOffset(off),
	}
}

func positionAtTime(t time.Time) position {
	return position{
		anchor: anchorAtTime(t),
	}
}

type testRegistry struct {
	registry *avroregistry.Registry
	srv      *httptest.Server
	faked    bool
	schema   string
	sub      string
	url      string
}

func newTestRegistry(c *qt.C) *testRegistry {
	ctx := context.Background()
	reg := &testRegistry{
		sub: randomString(10),
		url: os.Getenv(ENV_REGISTRY),
	}
	// If KT_REGISTRY is not explicitly set, we use a fake server.
	if reg.url == "" {
		reg.faked = true
		reg.srv = httptest.NewServer(http.HandlerFunc(reg.fakeServerHandler))
		reg.url = reg.srv.URL
	}
	var err error
	reg.registry, err = avroregistry.New(avroregistry.Params{
		ServerURL:     reg.url,
		RetryStrategy: retry.Regular{},
	})
	c.Assert(err, qt.IsNil)
	c.Defer(func() {
		err := reg.registry.DeleteSubject(ctx, reg.sub)
		c.Check(err, qt.IsNil)
		if reg.srv != nil {
			reg.srv.Close()
		}
	})
	return reg
}

func (reg *testRegistry) register(c *qt.C, schema *avro.Type) int64 {
	if reg.faked {
		reg.schema = schema.String()
		return 1
	}
	id, err := reg.registry.Register(context.Background(), reg.sub, schema)
	c.Assert(err, qt.IsNil)
	return id
}

func (reg *testRegistry) fakeServerHandler(w http.ResponseWriter, r *http.Request) {
	var body []byte
	if r.Method == http.MethodGet &&
		(strings.HasPrefix(r.RequestURI, "/schemas/ids") || strings.HasPrefix(r.RequestURI, "/subjects/")) {
		var err error
		body, err = json.Marshal(struct {
			ID      int    `json:"id"`
			Version int    `json:"version"`
			Schema  string `json:"schema"`
		}{ID: 1, Version: 1, Schema: reg.schema})
		if err != nil {
			panic(err)
		}
	}

	w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

// createAvroMessage is a helper to create Avro message.
// See https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format.
func createAvroMessage(schemaID int64, data []byte) []byte {
	b := []byte{0}        // magic byte
	id := make([]byte, 4) // 4-byte schema id
	binary.BigEndian.PutUint32(id, uint32(schemaID))
	b = append(b, id...)
	return append(b, data...)
}
