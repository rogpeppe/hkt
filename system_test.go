package main

import (
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/go-cmp/cmp"
	"github.com/rogpeppe/go-internal/testscript"
)

var updateFlag = flag.Bool("update", false, "update testscript scripts to correspond with actual output")

// TestMain allows the test binary to call the top level main
// function so that it can be invoked by the testscript tests.
func TestMain(m *testing.M) {
	os.Exit(testscript.RunMain(m, map[string]func() int{
		"kt": main1,
	}))
}

const (
	testBrokerAddr = "localhost:9092"
	testSecretsDir = "test-secrets"
)

func TestSystem(t *testing.T) {
	// Run all the scripts in testdata/*.txt which do end-to-end testing
	// of the top level command.
	//
	// We make some environment variables available the scripts,
	// including a random topic name so that the scripts can produce
	// to a topic without fear of clashes, the current time of day
	// so that the scripts can compare timestamp values against it,
	// and the address of the local Kafka broker.
	//
	// It makes a command available that does JSON comparison
	// (see the cmpenvjson docs).
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf(err.Error())
	}
	testscript.Run(t, testscript.Params{
		Dir: "testdata",
		Cmds: map[string]func(ts *testscript.TestScript, neg bool, args []string){
			"cmpenvjson": cmpenvjson,
		},
		Setup: func(e *testscript.Env) error {
			topic := randomString(6)
			e.Vars = append(e.Vars,
				"topic="+topic,
				ENV_BROKERS+"="+testBrokerAddr,
				"now="+time.Now().UTC().Format(time.RFC3339),
				ENV_AUTH+"="+path.Join(testSecretsDir, "auth.json"),
				"SECRETS_DIR="+path.Join(cwd, testSecretsDir),
				"GODEBUG=x509ignoreCN=0",
			)
			e.Defer(func() {
				if err := deleteTopic(topic); err != nil {
					t.Errorf("cannot delete topic %q from local Kafka: %v", topic, err)
				}
			})
			return nil
		},
		UpdateScripts: *updateFlag,
	})
}

func randomString(length int) string {
	buf := make([]byte, length)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Errorf("cannot generate random numbers: %v", err))
	}
	return fmt.Sprintf("hkt-%x", buf)
}

// cmpenvjson implements the cmpenvjson testscript command.
// Usage:
//	cmpenvjson file1 file2|object
// It succeeds if file1 has the same JSON contents
// as file2 after environment variables are substituted in file2.
// File2 can be a literal JSON object instead of a filename.
func cmpenvjson(ts *testscript.TestScript, neg bool, args []string) {
	if neg {
		ts.Fatalf("cmpjson does not support !")
	}
	if len(args) != 2 {
		ts.Fatalf("usage: cmpjson file file")
	}

	got := ts.ReadFile(args[0])
	var want string
	if strings.HasPrefix(args[1], "{") {
		want = args[1]
	} else {
		want = ts.ReadFile(args[1])
	}
	want = os.Expand(want, ts.Getenv)
	var gotv, wantv interface{}
	ts.Check(json.Unmarshal([]byte(got), &gotv))
	ts.Check(json.Unmarshal([]byte(want), &wantv))
	if diff := cmp.Diff(gotv, wantv, cmp.Comparer(func(s1, s2 string) bool {
		if s1 == s2 {
			return true
		}
		t1, err1 := time.Parse(time.RFC3339, s1)
		t2, err2 := time.Parse(time.RFC3339, s2)
		if err1 != nil || err2 != nil {
			return false
		}
		d := t1.Sub(t2)
		if d < 0 {
			d = -d
		}
		return d < 5*time.Second
	})); diff != "" {
		ts.Fatalf("files differ:\n%s\n", diff)
	}
}

func deleteTopic(topic string) error {
	cfg := sarama.NewConfig()
	cfg.Version = defaultKafkaVersion

	authCfg := authConfig{}
	err := readAuthFile(path.Join(testSecretsDir, "auth.json"), "", &authCfg)
	if err != nil {
		return err
	}
	err = setupAuthTLS(authCfg, cfg)
	if err != nil {
		return err
	}

	admin, err := sarama.NewClusterAdmin([]string{testBrokerAddr}, cfg)
	if err != nil {
		return err
	}
	defer admin.Close()
	if err := admin.DeleteTopic(topic); err != nil && err != sarama.ErrUnknownTopicOrPartition {
		return err
	}
	return nil
}
