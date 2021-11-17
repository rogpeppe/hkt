package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	qt "github.com/frankban/quicktest"
	"github.com/heetch/avro"
)

func TestProduceParseArgsUsesEnvVar(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	c.Setenv(ENV_BROKERS, "hans:2000")

	cmd0, _, err := parseCmd("hkt", "produce")
	c.Assert(err, qt.Equals, nil)
	cmd := cmd0.(*produceCmd)

	c.Assert(cmd.brokers(), qt.DeepEquals, []string{"hans:2000"})
}

// brokers default to localhost:9092
func TestProduceParseArgsDefault(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	c.Setenv(ENV_BROKERS, "")

	cmd0, _, err := parseCmd("hkt", "produce")
	c.Assert(err, qt.Equals, nil)
	cmd := cmd0.(*produceCmd)
	c.Assert(cmd.brokers(), qt.DeepEquals, []string{"localhost:9092"})
}

func TestProduceParseArgsFlagsOverrideEnv(t *testing.T) {
	c := qt.New(t)
	defer c.Done()

	// command line arg wins
	c.Setenv(ENV_BROKERS, "BLABB")

	cmd0, _, err := parseCmd("hkt", "produce", "-brokers", "hans:2000")
	c.Assert(err, qt.Equals, nil)
	cmd := cmd0.(*produceCmd)
	c.Assert(cmd.brokers(), qt.DeepEquals, []string{"hans:2000"})
}

func TestMakeSaramaMessage(t *testing.T) {
	c := qt.New(t)
	mustDecodersForType := func(p *produceCmd, typ string) (func(json.RawMessage) ([]byte, error), func(json.RawMessage) ([]byte, error)) {
		keyDec, err := p.decoderForType("key", typ)
		c.Assert(err, qt.IsNil)
		valueDec, err := p.decoderForType("value", typ)
		c.Assert(err, qt.IsNil)
		return keyDec, valueDec
	}

	target := &produceCmd{}
	target.decodeKey, target.decodeValue = mustDecodersForType(target, "string")
	key, value := `"key"`, `"value"`
	msg := producerMessage{Key: json.RawMessage(key), Value: json.RawMessage(value)}
	actual, err := target.makeSaramaMessage(msg)
	c.Assert(err, qt.Equals, nil)
	c.Assert(encoderStr(actual.Key), qt.Equals, "key")
	c.Assert(encoderStr(actual.Value), qt.Equals, "value")

	target.decodeKey, target.decodeValue = mustDecodersForType(target, "hex")
	key, value = `"41"`, `"42"`
	msg = producerMessage{Key: json.RawMessage(key), Value: json.RawMessage(value)}
	actual, err = target.makeSaramaMessage(msg)
	c.Assert(err, qt.Equals, nil)
	c.Assert(encoderStr(actual.Key), qt.Equals, "A")
	c.Assert(encoderStr(actual.Value), qt.Equals, "B")

	target.decodeKey, target.decodeValue = mustDecodersForType(target, "base64")
	key, value = `"aGFucw=="`, `"cGV0ZXI="`
	msg = producerMessage{Key: json.RawMessage(key), Value: json.RawMessage(value)}
	actual, err = target.makeSaramaMessage(msg)
	c.Assert(err, qt.Equals, nil)
	c.Assert(encoderStr(actual.Key), qt.Equals, "hans")
	c.Assert(encoderStr(actual.Value), qt.Equals, "peter")

	target.decodeKey, target.decodeValue = mustDecodersForType(target, "json")
	key, value = `{"x":1}`, `[1,2]`
	msg = producerMessage{Key: json.RawMessage(key), Value: json.RawMessage(value)}
	actual, err = target.makeSaramaMessage(msg)
	c.Assert(err, qt.Equals, nil)
	c.Assert(encoderStr(actual.Key), qt.Equals, `{"x":1}`)
	c.Assert(encoderStr(actual.Value), qt.Equals, `[1,2]`)

	c.Run("avro", func(c *qt.C) {
		type record struct {
			A int
			B int
		}

		_, wType, err := avro.Marshal(record{})
		c.Assert(err, qt.IsNil)

		reg := newTestRegistry(c)
		schemaID := reg.register(c, wType)

		cmd := produceCmd{
			coder: coder{
				topic:        "a",
				avroRegistry: reg.registry,
			},
		}

		cmd.decodeKey, err = cmd.decoderForType("key", "string")
		c.Assert(err, qt.IsNil)
		cmd.decodeValue, err = cmd.decoderForType("value", "avro")
		c.Assert(err, qt.IsNil)

		key, value := `"12"`, `{"A": 40, "B": 20}`
		msg := producerMessage{Key: json.RawMessage(key), Value: json.RawMessage(value)}
		actual, err := cmd.makeSaramaMessage(msg)
		c.Assert(err, qt.IsNil)
		c.Assert(encoderStr(actual.Key), qt.Equals, "12")
		// In the byte slice below:
		//       0: Starting schema ID magic byte
		//       1: 4 bytes Schema ID
		//	80: A=40
		//	40: B=20
		expectedBlob := []byte{0, 0, 0, 0, byte(uint8(schemaID)), 80, 40}
		gotValue, err := actual.Value.Encode()
		c.Assert(err, qt.IsNil)
		c.Assert(gotValue, qt.ContentEquals, expectedBlob)
	})
}

func TestDeserializeLines(t *testing.T) {
	data := []struct {
		in             string
		literal        bool
		partitionCount int32
		expected       []producerMessage
	}{{
		in:             "",
		literal:        false,
		partitionCount: 1,
		expected:       nil,
	}, {
		in:             `{"key":"hans","value":"123"}`,
		literal:        false,
		partitionCount: 4,
		expected: []producerMessage{
			newMessage(`"hans"`, `"123"`, -1),
		},
	}, {
		in:             `{"key":"hans","value":"123","partition":1}`,
		literal:        false,
		partitionCount: 3,
		expected:       []producerMessage{newMessage(`"hans"`, `"123"`, 1)},
	}, {
		in:             `{"other":"json","values":"avail"}`,
		literal:        true,
		partitionCount: 4,
		expected:       []producerMessage{newMessage("", `"{\"other\":\"json\",\"values\":\"avail\"}"`, -1)},
	}, {
		in:             `so lange schon`,
		literal:        false,
		partitionCount: 3,
		expected:       nil,
	}}

	c := qt.New(t)
	for i, d := range data {
		c.Run(fmt.Sprint(i), func(c *qt.C) {
			target := &produceCmd{
				literal: d.literal,
			}
			in := make(chan string, 1)
			out := make(chan producerMessage)
			go target.deserializeLines(in, out, d.partitionCount)
			in <- d.in
			close(in)
			var msgs []producerMessage
			for m := range out {
				msgs = append(msgs, m)
			}
			c.Assert(msgs, deepEquals, d.expected)
		})
	}
}

func encoderStr(enc sarama.Encoder) string {
	data, err := enc.Encode()
	if err != nil {
		panic(err)
	}
	return string(data)
}

func newMessage(key, value string, partition int32) producerMessage {
	var m producerMessage
	if key != "" {
		m.Key = json.RawMessage(key)
	}
	if value != "" {
		m.Value = json.RawMessage(value)
	}
	if partition >= 0 {
		m.Partition = &partition
	}
	return m
}
