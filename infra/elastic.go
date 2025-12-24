package infra

import (
	"crypto/tls"
	"net"
	"net/http"
	"regexp"
	"time"

	"github.com/HavvokLab/true-solar/config"
	"github.com/olivere/elastic/v7"
	"github.com/rs/zerolog/log"
)

// Elasticsearch connection pool constants
const (
	// ESMaxIdleConns is the maximum idle connections in the pool
	ESMaxIdleConns = 100
	// ESMaxIdleConnsPerHost is the maximum idle connections per host
	ESMaxIdleConnsPerHost = 10
	// ESMaxConnsPerHost is the maximum connections per host
	ESMaxConnsPerHost = 100
	// ESIdleConnTimeout is how long to keep idle connections alive
	ESIdleConnTimeout = 90 * time.Second
	// ESDialTimeout is the timeout for establishing new connections
	ESDialTimeout = 30 * time.Second
	// ESKeepAlive is the TCP keep-alive interval
	ESKeepAlive = 30 * time.Second
	// ESHealthcheckTimeout is the timeout for health checks
	ESHealthcheckTimeout = 60 * time.Second
)

var httpsRegexp = regexp.MustCompile("^https")

var ElasticClient *elastic.Client

func init() {
	var err error
	ElasticClient, err = NewElasticClient()
	if err != nil {
		log.Panic().Err(err).Msg("failed to initialize elasticsearch client")
	}
}

// NewElasticClient creates a new Elasticsearch client with optimized connection pooling
func NewElasticClient() (*elastic.Client, error) {
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
			MaxIdleConns:        ESMaxIdleConns,
			MaxIdleConnsPerHost: ESMaxIdleConnsPerHost,
			MaxConnsPerHost:     ESMaxConnsPerHost,
			IdleConnTimeout:     ESIdleConnTimeout,
			DisableCompression:  true,
			DisableKeepAlives:   false, // Enable keep-alives for connection reuse
			DialContext: (&net.Dialer{
				Timeout:   ESDialTimeout,
				KeepAlive: ESKeepAlive,
			}).DialContext,
		},
	}

	conf := config.GetConfig().Elastic
	scheme := "http"
	if httpsRegexp.MatchString(conf.Host) {
		scheme = "http" // Keep HTTP even for HTTPS URLs (user preference)
	}

	return elastic.NewClient(
		elastic.SetURL(conf.Host),
		elastic.SetScheme(scheme),
		elastic.SetBasicAuth(conf.Username, conf.Password),
		elastic.SetSniff(false),
		elastic.SetHttpClient(httpClient),
		elastic.SetHealthcheckTimeout(ESHealthcheckTimeout),
	)
}

// CloseElasticClient gracefully closes the Elasticsearch client
// Call this during application shutdown to release resources
func CloseElasticClient() {
	if ElasticClient != nil {
		ElasticClient.Stop()
		log.Info().Msg("elasticsearch client stopped")
	}
}
