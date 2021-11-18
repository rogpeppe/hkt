package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"os/user"
	"regexp"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/heetch/avro"
	"github.com/heetch/avro/avroregistry"
	goavro "github.com/linkedin/goavro/v2"
	"golang.org/x/crypto/ssh/terminal"
)

const (
	ENV_AUTH          = "KT_AUTH"
	ENV_ADMIN_TIMEOUT = "KT_ADMIN_TIMEOUT"
	ENV_BROKERS       = "KT_BROKERS"
	ENV_TOPIC         = "KT_TOPIC"
	ENV_REGISTRY      = "KT_REGISTRY"
)

var (
	invalidClientIDCharactersRegExp = regexp.MustCompile(`[^a-zA-Z0-9_-]`)
)

func listenForInterrupt(q chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	sig := <-signals
	fmt.Fprintf(os.Stderr, "received signal %s\n", sig)
	close(q)
}

var defaultKafkaVersion = sarama.V2_0_0_0

type commonFlags struct {
	verbose      bool
	version      sarama.KafkaVersion
	tlsRequested bool
	auth         authConfig
	authFile     string

	brokerStrs []string
}

func (f *commonFlags) brokers() []string {
	brokers := append([]string{}, f.brokerStrs...)
	for i, b := range f.brokerStrs {
		if !strings.Contains(b, ":") {
			brokers[i] = b + ":9092"
		}
	}
	return brokers
}

func (f *commonFlags) addFlags(flags *flag.FlagSet) {
	f.brokerStrs = []string{"localhost"}
	f.version = defaultKafkaVersion
	flags.Var(listFlag{&f.brokerStrs}, "brokers", "Comma-separated list of brokers.  Each broker definition may optionally contain a port number. The port defaults to 9092 when omitted.")
	flags.Var(kafkaVersionFlag{v: &f.version}, "version", "Kafka protocol version")
	flags.BoolVar(&f.tlsRequested, "tls", false, "Request server-side TLS without client-side.")
	flags.StringVar(&f.authFile, "auth", "", "Path to auth configuration file. It can also be set via KT_AUTH env variable")
	flags.BoolVar(&f.verbose, "verbose", false, "More verbose logging to stderr.")
}

func (f *commonFlags) saramaConfig(name string) (*sarama.Config, error) {
	cfg := sarama.NewConfig()
	cfg.Version = f.version
	usr, err := user.Current()
	var username string
	if err != nil {
		warningf("failed to read current user name: %v", err)
		username = "anon"
	} else {
		username = usr.Username
	}
	cfg.ClientID = "kt-" + name + "-" + sanitizeUsername(username)

	if err = readAuthFile(f.authFile, os.Getenv(ENV_AUTH), &f.auth); err != nil {
		return nil, fmt.Errorf("failed to read auth file: %w", err)
	}

	if err = setupAuth(f.auth, cfg); err != nil {
		return nil, fmt.Errorf("failed to setup auth: %w", err)
	}
	if f.verbose {
		fmt.Fprintf(os.Stderr, "sarama client configuration %#v\n", cfg)
	}
	return cfg, nil
}

type listFlag struct {
	v *[]string
}

func (v listFlag) String() string {
	if v.v == nil {
		return ""
	}
	return strings.Join(*v.v, ",")
}

func (v listFlag) Set(s string) error {
	if s == "" {
		*v.v = nil
	} else {
		*v.v = strings.Split(s, ",")
	}
	return nil
}

type kafkaVersionFlag struct {
	v *sarama.KafkaVersion
}

func (v kafkaVersionFlag) String() string {
	if v.v == nil {
		return ""
	}
	return v.v.String()
}

func (v kafkaVersionFlag) Set(s string) error {
	vers, err := sarama.ParseKafkaVersion(strings.TrimPrefix(s, "v"))
	if err != nil {
		return fmt.Errorf("invalid kafka version %q: %v", s, err)
	}
	*v.v = vers
	return nil
}

func logClose(name string, c io.Closer) {
	if err := c.Close(); err != nil {
		warningf("failed to close %#v: %v", name, err)
	}
}

type printer struct {
	mu      sync.Mutex
	marshal func(interface{}) ([]byte, error)
}

func newPrinter(pretty bool) *printer {
	marshal := json.Marshal
	if pretty && terminal.IsTerminal(1) {
		marshal = func(i interface{}) ([]byte, error) { return json.MarshalIndent(i, "", "  ") }
	}
	return &printer{
		marshal: marshal,
	}
}

func (p *printer) print(val interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	buf, err := p.marshal(val)
	if err != nil {
		warningf("failed to marshal output %#v: %v", val, err)
	}
	fmt.Println(string(buf))
}

func warningf(f string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, "hkt: warning: %s\n", fmt.Sprintf(f, a...))
}

func readStdinLines(max int, out chan string) {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(nil, max)
	for scanner.Scan() {
		out <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		warningf("error reading standard input: %v", err)
	}
	close(out)
}

func sanitizeUsername(u string) string {
	// Windows user may have format "DOMAIN|MACHINE\username", remove domain/machine if present
	s := strings.Split(u, "\\")
	u = s[len(s)-1]
	// Windows account can contain spaces or other special characters not supported
	// in client ID. Keep the bare minimum and ditch the rest.
	return invalidClientIDCharactersRegExp.ReplaceAllString(u, "")
}

// setUpCerts takes the paths to a tls certificate, CA, and certificate key in
// a PEM format and returns a constructed tls.Config object.
func setUpCerts(certPath, caPath, keyPath string) (*tls.Config, error) {
	if certPath == "" && caPath == "" && keyPath == "" {
		return nil, nil
	}

	if certPath == "" || caPath == "" || keyPath == "" {
		return nil, fmt.Errorf("certificate, CA and key path are required - got cert=%#v ca=%#v key=%#v", certPath, caPath, keyPath)
	}

	caString, err := ioutil.ReadFile(caPath)
	if err != nil {
		return nil, err
	}

	caPool := x509.NewCertPool()
	ok := caPool.AppendCertsFromPEM(caString)
	if !ok {
		return nil, fmt.Errorf("unable to add cert at %s to certificate pool", caPath)
	}

	clientCert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}

	bundle := &tls.Config{
		RootCAs:      caPool,
		Certificates: []tls.Certificate{clientCert},
	}
	bundle.BuildNameToCertificate()
	return bundle, nil
}

// setFlagsFromEnv sets unset flags in fs from environment
// variables as specified by the flags map, which maps
// from flag name to the environment variable for that name.
//
// If a flag f is part of fs but has not been explicitly set on the
// command line, and flags[f] exists, then it will
// be set from os.Getenv(flags[f]).
func setFlagsFromEnv(fs *flag.FlagSet, flags map[string]string) error {
	set := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		set[f.Name] = true
	})
	for name, env := range flags {
		f := fs.Lookup(name)
		if f == nil {
			panic(fmt.Errorf("flag %q ($%s) not found", name, env))
		}
		if set[name] {
			continue
		}
		if v := os.Getenv(env); v != "" {
			if err := f.Value.Set(v); err != nil {
				return fmt.Errorf("cannot parse $%s as -%s flag value: %v", env, name, err)
			}
		}
	}
	return nil
}

// coder implements the common data required for encoding/decoding data
type coder struct {
	topic       string
	registryURL string

	registry *avroregistry.Registry
}

// addFlags adds flags required for encoding and decoding.
func (c *coder) addFlags(flags *flag.FlagSet) {
	flags.StringVar(&c.registryURL, "registry", "", "The Avro schema registry server URL.")
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
		// Subject using Kafka Schema Registry for value
		subject := c.topic + "-" + keyOrValue

		ctx := context.Background()
		sch, err := c.registry.Schema(ctx, subject, "latest")
		if err != nil {
			return nil, err
		}

		// TODO: Cache schema
		t, err := avro.ParseType(sch.Schema)
		if err != nil {
			return nil, err
		}

		// Canonicalize the schema to remove default values and logical types
		// to work around https://github.com/linkedin/goavro/issues/198
		inCodec, err := goavro.NewCodec(t.CanonicalString(0))
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

		enc := c.registry.Encoder(subject)
		buf := make([]byte, 0, 512)
		buf = enc.AppendSchemaID(buf, sch.ID)

		return append(buf, inBlob...), nil
	}
}

var nullJSON = json.RawMessage("null")

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

type authConfig struct {
	Mode              string `json:"mode"`
	CACert            string `json:"ca-certificate"`
	ClientCert        string `json:"client-certificate"`
	ClientCertKey     string `json:"client-certificate-key"`
	SASLPlainUser     string `json:"sasl_plain_user"`
	SASLPlainPassword string `json:"sasl_plain_password"`
}

func setupAuth(auth authConfig, saramaCfg *sarama.Config) error {
	if auth.Mode == "" {
		return nil
	}

	switch auth.Mode {
	case "TLS":
		return setupAuthTLS(auth, saramaCfg)
	case "TLS-1way":
		return setupAuthTLS1Way(auth, saramaCfg)
	case "SASL":
		return setupSASL(auth, saramaCfg)
	default:
		return fmt.Errorf("unsupport auth mode: %#v", auth.Mode)
	}
}

func setupSASL(auth authConfig, saramaCfg *sarama.Config) error {
	saramaCfg.Net.SASL.Enable = true
	saramaCfg.Net.SASL.User = auth.SASLPlainUser
	saramaCfg.Net.SASL.Password = auth.SASLPlainPassword
	return nil
}

func setupAuthTLS1Way(auth authConfig, saramaCfg *sarama.Config) error {
	saramaCfg.Net.TLS.Enable = true
	saramaCfg.Net.TLS.Config = &tls.Config{}
	return nil
}

func setupAuthTLS(auth authConfig, saramaCfg *sarama.Config) error {
	if auth.CACert == "" || auth.ClientCert == "" || auth.ClientCertKey == "" {
		return fmt.Errorf("client-certificate, client-certificate-key and ca-certificate are required - got auth=%#v", auth)
	}

	caString, err := ioutil.ReadFile(auth.CACert)
	if err != nil {
		return fmt.Errorf("failed to read ca-certificate err=%v", err)
	}

	caPool := x509.NewCertPool()
	ok := caPool.AppendCertsFromPEM(caString)
	if !ok {
		return fmt.Errorf("unable to add ca-certificate at %s to certificate pool", auth.CACert)
	}

	clientCert, err := tls.LoadX509KeyPair(auth.ClientCert, auth.ClientCertKey)
	if err != nil {
		return err
	}

	tlsCfg := &tls.Config{RootCAs: caPool, Certificates: []tls.Certificate{clientCert}}
	tlsCfg.BuildNameToCertificate()

	saramaCfg.Net.TLS.Enable = true
	saramaCfg.Net.TLS.Config = tlsCfg

	return nil
}

func readAuthFile(argFN string, envFN string, target *authConfig) error {
	if argFN == "" && envFN == "" {
		return nil
	}

	fn := argFN
	if fn == "" {
		fn = envFN
	}

	byts, err := ioutil.ReadFile(fn)
	if err != nil {
		return fmt.Errorf("failed to read auth file: %w", err)
	}

	if err := json.Unmarshal(byts, target); err != nil {
		return fmt.Errorf("failed to unmarshal auth file: %w", err)
	}

	return nil
}
