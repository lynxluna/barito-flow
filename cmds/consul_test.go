package cmds

//
import (
	"net/http"
	"os"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestConsulElasticsearch(t *testing.T) {
	ts := NewTestServer(http.StatusOK, []byte(`[
	{
		"ServiceAddress": "172.17.0.3",
		"ServicePort": 5000,
		"ServiceMeta": {
        "http_schema": "https"
    }
	}
]`))
	defer ts.Close()

	os.Setenv(EnvConsulUrl, ts.URL)
	defer os.Clearenv()

	url, err := consulElasticsearchUrl()
	FatalIfError(t, err)
	FatalIf(t, url != "https://172.17.0.3:5000", "wrong url")
}

func TestConsulElasticsearch_NoHttpSchema(t *testing.T) {
	ts := NewTestServer(http.StatusOK, []byte(`[
	{
		"ServiceAddress": "172.17.0.3",
		"ServicePort": 5000
	}
]`))
	defer ts.Close()

	os.Setenv(EnvConsulUrl, ts.URL)
	defer os.Clearenv()

	url, err := consulElasticsearchUrl()
	FatalIfError(t, err)
	FatalIf(t, url != "http://172.17.0.3:5000", "wrong url")
}

func TestConsulElasticsearch_NoService(t *testing.T) {
	ts := NewTestServer(http.StatusOK, []byte(`[]`))
	defer ts.Close()

	os.Setenv(EnvConsulUrl, ts.URL)
	defer os.Clearenv()

	_, err := consulElasticsearchUrl()
	FatalIfWrongError(t, err, "No Service")
}

func TestGetKafkaBorkersFromConsul_NoEnv(t *testing.T) {
	_, err := consulKafkaBroker()
	FatalIfWrongError(t, err, "no ENV BARITO_CONSUL_URL")
}

func TestConsulKafkaBorkers(t *testing.T) {
	ts := NewTestServer(http.StatusOK, []byte(`[
  {
    "ServiceAddress": "172.17.0.3",
    "ServicePort": 5000
  },
  {
    "ServiceAddress": "172.17.0.4",
    "ServicePort": 5001
  }
]`))
	defer ts.Close()

	os.Setenv(EnvConsulUrl, ts.URL)
	defer os.Clearenv()

	brokers, err := consulKafkaBroker()
	FatalIfError(t, err)
	FatalIf(t, len(brokers) != 2, "return wrong brokers")
	FatalIf(t, brokers[0] != "172.17.0.3:5000", "return wrong brokers[0]")
	FatalIf(t, brokers[1] != "172.17.0.4:5001", "return wrong brokers[1]")
}
