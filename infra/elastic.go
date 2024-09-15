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

var httpsRegexp = regexp.MustCompile("^https")

var ElasticClient *elastic.Client

func init() {
	var err error
	ElasticClient, err = NewElasticClient()
	if err != nil {
		log.Panic().Err(err).Msg("failed to initialize elasticsearch client")
	}
}

func NewElasticClient() (*elastic.Client, error) {
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:    &tls.Config{InsecureSkipVerify: true},
			MaxIdleConns:       10,
			IdleConnTimeout:    30 * time.Second,
			DisableCompression: true,
			DisableKeepAlives:  true,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}

	conf := config.GetConfig().Elastic
	scheme := "http"
	if httpsRegexp.FindString(conf.Host) != "" {
		scheme = "http"
	}

	return elastic.NewClient(
		elastic.SetURL(conf.Host),
		elastic.SetScheme(scheme),
		elastic.SetBasicAuth(conf.Username, conf.Password),
		elastic.SetSniff(false),
		elastic.SetHttpClient(httpClient),
		elastic.SetHealthcheckTimeout(time.Duration(300)*time.Second),
	)
}
