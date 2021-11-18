package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"

	"github.com/heetch/avro"
	"github.com/heetch/avro/avroregistry"
	goavro "github.com/linkedin/goavro/v2"
)

// coder implements the common data required for encoding/decoding data
type coder struct {
	topic          string
	registryURL    string
	avscFile       string
	avroRecordName string

	avroRegistry *avroregistry.Registry
	avroSchema   *avro.Type
	avroSchemaID int64
}

// addFlags adds flags required for encoding and decoding.
func (c *coder) addFlags(flags *flag.FlagSet) {
	flags.StringVar(&c.registryURL, "registry", "", "The Avro schema registry server URL.")
	flags.StringVar(&c.avscFile, "value-avro-schema", "", `Path to AVSC file to format the file. If it is set, then -valuecodec is set to "avro"`)
	flags.StringVar(&c.avroRecordName, "value-avro-record-name", "", "Record name to use when using TopicRecordNameStrategy to find the schema subject in Schema Registry")
}

// decoderForType returns a function to decode key or value depending of the expected format defined in typ
func (c *coder) decoderForType(keyOrValue, typ string) (func(m json.RawMessage) ([]byte, error), error) {
	var dec func(s string) ([]byte, error)
	switch typ {
	case "json":
		// Easy case - we already have the JSON-marshaled data.
		return func(m json.RawMessage) ([]byte, error) {
			return m, nil
		}, nil
	case "hex":
		dec = hex.DecodeString
	case "base64":
		dec = base64.StdEncoding.DecodeString
	case "string":
		dec = func(s string) ([]byte, error) {
			return []byte(s), nil
		}
	case "avro":
		return c.makeAvroDecoder(keyOrValue), nil
	default:
		return nil, fmt.Errorf(`unsupported decoder %#v, only json, string, hex and base64 are supported`, typ)
	}
	return func(m json.RawMessage) ([]byte, error) {
		var s string
		if err := json.Unmarshal(m, &s); err != nil {
			return nil, err
		}
		return dec(s)
	}, nil
}

func (c *coder) makeAvroDecoder(keyOrValue string) func(m json.RawMessage) ([]byte, error) {
	return func(m json.RawMessage) ([]byte, error) {
		// Check strategy to use
		// Subject using Kafka Schema Registry for value TopicNameStrategy
		subject := c.topic + "-" + keyOrValue
		if c.avroRecordName != "" {
			// Use TopicRecordNameStrategy
			subject = c.topic + "-" + c.avroRecordName
		}

		enc := c.avroRegistry.Encoder(subject)

		if c.avroSchemaID == 0 {
			ctx := context.Background()
			if c.avroSchema == nil {
				sch, err := c.avroRegistry.Schema(ctx, subject, "latest")
				if err != nil {
					return nil, err
				}

				t, err := avro.ParseType(sch.Schema)
				if err != nil {
					return nil, err
				}

				// Caching avroSchemaID for next calls
				c.avroSchema = t
				c.avroSchemaID = sch.ID
			} else {
				id, err := enc.IDForSchema(ctx, c.avroSchema)
				if err != nil {
					return nil, err
				}
				c.avroSchemaID = id
			}
		}

		// Canonicalize the schema to remove default values and logical types
		// to work around https://github.com/linkedin/goavro/issues/198
		inCodec, err := goavro.NewCodec(c.avroSchema.CanonicalString(0))
		if err != nil {
			return nil, err
		}

		inNative, _, err := inCodec.NativeFromTextual(m)
		if err != nil {
			return nil, err
		}

		inBlob, err := inCodec.BinaryFromNative(nil, inNative)
		if err != nil {
			return nil, err
		}

		buf := make([]byte, 0, 512)
		buf = enc.AppendSchemaID(buf, c.avroSchemaID)

		return append(buf, inBlob...), nil
	}
}

// loadSchemaFile loads AVSC file from the given path and it is stored in avroSchema
func (c *coder) loadSchemaFile(path string) error {
	blob, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read AVSC file: %w", err)
	}

	t, err := avro.ParseType(string(blob))
	if err != nil {
		return fmt.Errorf("failed to parse AVSC file: %w", err)
	}

	c.avroSchema = t

	return nil
}

// encoderForType returns a function to encode key or value depending of the expected format defined in typ
func (c *coder) encoderForType(keyOrValue, typ string) (func([]byte) (json.RawMessage, error), error) {
	var enc func([]byte) string
	switch typ {
	case "json":
		return func(data []byte) (json.RawMessage, error) {
			if err := json.Unmarshal(data, new(json.RawMessage)); err != nil {
				return nil, fmt.Errorf("invalid JSON value %q: %v", data, err)
			}
			return json.RawMessage(data), nil
		}, nil
	case "hex":
		enc = hex.EncodeToString
	case "base64":
		enc = base64.StdEncoding.EncodeToString
	case "string":
		enc = func(data []byte) string {
			return string(data)
		}
	case "avro":
		return c.encodeAvro, nil
	default:
		return nil, fmt.Errorf(`unsupported decoder %#v, only json, string, hex, base64 and avro are supported`, typ)
	}
	return func(data []byte) (json.RawMessage, error) {
		if data == nil {
			return nullJSON, nil
		}
		data1, err := json.Marshal(enc(data))
		if err != nil {
			// marshaling a string cannot fail but be defensive.
			return nil, err
		}
		return json.RawMessage(data1), nil
	}, nil
}

func (c *coder) encodeAvro(data []byte) (json.RawMessage, error) {
	dec := c.avroRegistry.Decoder()
	id, body := dec.DecodeSchemaID(data)
	if body == nil {
		return nil, fmt.Errorf("cannot decode schema id")
	}
	// TODO: cache the schema
	schema, err := dec.SchemaForID(context.Background(), id)
	if err != nil {
		return nil, fmt.Errorf("cannot get schema for id %d: %v", id, err)
	}
	// Canonicalize the schema to remove default values and logical types
	// to work around https://github.com/linkedin/goavro/issues/198
	codec, err := goavro.NewCodec(schema.CanonicalString(0))
	if err != nil {
		return nil, fmt.Errorf("cannot create codec from schema %s: %v", schema, err)
	}
	native, _, err := codec.NativeFromBinary(body)
	if err != nil {
		return nil, fmt.Errorf("cannot convert native from binary: %v", err)
	}
	textual, err := codec.TextualFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("cannot convert textual from native: %v", err)
	}
	return json.RawMessage(textual), nil
}
