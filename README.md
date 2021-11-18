# hkt - a Kafka tool that likes JSON [![Build Status](https://travis-ci.org/heetch/hkt.svg?branch=master)](https://travis-ci.org/heetch/hkt)

_This is a fork of KT that we maintain, adding features oriented toward debugging. More details will come soon._

Some reasons why you might be interested:

* Consume messages on specific partitions between specific offsets.
* JSON/Avro/String formatted messages consumption with optional filtering by key
* JSON/Avro/String formatted messages production
* Display topic information (e.g., with partition offset and leader info).
* Modify consumer group offsets (e.g., resetting or manually setting offsets per topic and per partition).
* JSON output for easy consumption with tools like [kp](https://github.com/echojc/kp) or [jq](https://stedolan.github.io/jq/).
* JSON input to facilitate automation via tools like [jsonify](https://github.com/fgeller/jsonify).
* Configure brokers, schema registry, and authentication with the
  `KT_BROKERS`, `KT_REGISTRY` and `KT_AUTH` environment variables.
* Fast start up time.
* No buffering of output.
* Binary keys and payloads can be passed and presented in base64 or hex encoding.
* Support for TLS authentication.
* Basic cluster admin functions: Create & delete topics.

## Examples

<details><summary>Read details about topics that match a regex</summary>

```sh
$ hkt topic -filter news -partitions
{
  "name": "actor-news",
  "partitions": [
    {
      "id": 0,
      "oldest": 0,
      "newest": 0
    }
  ]
}
```
</details>

<details><summary>Produce messages</summary>

```sh
$ echo 'Alice wins Oscar' | hkt produce -topic actor-news -literal
{
  "count": 1,
  "partition": 0,
  "startOffset": 0
}

$ echo 'Bob wins Oscar' | kt produce -topic actor-news -literal
{
  "count": 1,
  "partition": 0,
  "startOffset": 0
}
$ for i in {6..9} ; do echo Bourne sequel $i in production. | hkt produce -topic actor-news -literal ;done
{
  "count": 1,
  "partition": 0,
  "startOffset": 1
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 2
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 3
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 4
}
```
</details>

<details><summary>Or pass in JSON object to control key, value and partition</summary>

```sh
$ echo '{"value": "Terminator terminated", "key": "Arni", "partition": 0}' | hkt produce -topic actor-news
{
  "count": 1,
  "partition": 0,
  "startOffset": 5
}
```
</details>

<details><summary>Read messages at specific offsets on specific partitions</summary>

```sh
$ hkt consume -topic actor-news -offsets 0=1:2
{
  "partition": 0,
  "offset": 1,
  "key": "",
  "value": "Bourne sequel 6 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
{
  "partition": 0,
  "offset": 2,
  "key": "",
  "value": "Bourne sequel 7 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
```
</details>

<details><summary>Follow a topic, starting relative to newest offset</summary>

```sh
$ hkt consume -topic actor-news -offsets all=newest-1:
{
  "partition": 0,
  "offset": 4,
  "key": "",
  "value": "Bourne sequel 9 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
{
  "partition": 0,
  "offset": 5,
  "key": "Arni",
  "value": "Terminator terminated",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
^Creceived interrupt - shutting down
shutting down partition consumer for partition 0
```
</details>

<details><summary>View offsets for a given consumer group</summary>

```sh
$ hkt group -group enews -topic actor-news -partitions 0
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 6,
      "lag": 0
    }
  ]
}
```
</details>

<details><summary>Change consumer group offset</summary>

```sh
$ hkt group -group enews -topic actor-news -partitions 0 -reset 1
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 1,
      "lag": 5
    }
  ]
}
$ hkt group -group enews -topic actor-news -partitions 0
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 1,
      "lag": 5
    }
  ]
}
```
</details>

<details><summary>Create and delete a topic</summary>

```sh
$ hkt admin -createtopic morenews -topicdetail <(jsonify =NumPartitions 1 =ReplicationFactor 1)
$ hkt topic -filter news
{
  "name": "morenews"
}
$ hkt admin -deletetopic morenews
$ hkt topic -filter news
```

</details>

<details><summary>Change broker address via environment variable</summary>

```sh
$ export KT_BROKERS=brokers.kafka:9092
$ hkt <command> <option>
```

</details>

<details><summary>Produce and consume Avro formatted messages</summary>

Provided the following schema for this `subject` `actors-value` in Schema Registry `http://localhost:8081`:

```json
{"type": "record", "name": "Actor", "fields": [{"type": "string", "name": "FirstName"}]}
```

It produces the Avro formatted message:

```sh
$ echo '{"value": {"FirstName": "Ryan"}, "key": "id-42"}' | hkt produce -topic actors -registry http://localhost:8081 -valuecodec avro
```

It can be also send using a file:
```sh
$ echo '{"value": {"FirstName": "Arnold"}, "key": "id-43"}' | hkt produce -topic actors -registry http://localhost:8081 -value-avro-schema Actor.avsc
```

That they may be consumed:
```sh
$ hkt consume -registry http://localhost:8081 -valuecodec avro actors
{
  "partition": 0,
  "offset": 0,
  "key": "id-42",
  "value": {
    "FirstName": "Ryan"
  },
  "time": "2021-11-16T01:01:01Z"
}
{
  "partition": 0,
  "offset": 1,
  "key": "id-43",
  "value": {
    "FirstName": "Arnold"
  },
  "time": "2021-11-17T01:01:01Z"
}
```

Not only `TopicNameStrategy` is supported but `TopicRecordNameStrategy` is supported to find the subject name for
a schema using `-value-avro-record-name`. This can be used in conjunction with `-value-avro-schema` or get
latest version of the schema from the Schema Registry without providing any parameter.

</details>


## Installation

You can download hkt via the [Releases](https://github.com/heetch/hkt/releases) section.

Alternatively, the usual way via the go tool, for example:

    $ GOMODULE111=off go get -u github.com/heetch/hkt

## Usage:

    $ hkt -help
    hkt is a tool for Kafka.

    Usage:

            hkt command [arguments]

    The commands are:

            consume        consume messages.
            produce        produce messages.
            topic          topic information.
            group          consumer group information and modification.
            admin          basic cluster administration.

    Use "hkt [command] -help" for for information about the command.

    Authentication:

    Authentication with Kafka can be configured via a JSON file.
    You can set the file name via an "-auth" flag to each command or
    set it via the environment variable KT_AUTH.

## Authentication / Encryption

Authentication configuration is possibly via a JSON file. You indicate the mode
of authentication you need and provide additional information as required for
your mode. You pass the path to your configuration file via the `-auth` flag to
each command individually, or set it via the environment variable `KT_AUTH`.

### TLS

Required fields:

 - `mode`: This needs to be set to `TLS`
 - `client-certificate`: Path to your certificate
 - `client-certificate-key`: Path to your certificate key
 - `ca-certificate`: Path to your CA certificate

Example for an authorization configuration that is used for the system tests:


    {
        "mode": "TLS",
        "client-certificate": "test-secrets/kt-test.crt",
        "client-certificate-key": "test-secrets/kt-test.key",
        "ca-certificate": "test-secrets/snakeoil-ca-1.crt"
    }

### TLS one-way

Required fields:

 - `mode`: This needs to be set to `TLS-1way`

Example:


    {
        "mode": "TLS-1way",
    }

### Other modes

Please create an
[issue](https://github.com/heetch/hkt/issues/new) with details for the mode that you need.


