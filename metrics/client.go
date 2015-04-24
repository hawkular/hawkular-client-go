package metrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// MetricType restrictions
type MetricType int

const (
	Numeric = iota
	Availability
	Counter
)

var longForm = []string{
	"gauges",
	"availability",
	"counter",
}

var shortForm = []string{
	"gauge",
	"availability",
	"counter",
}

func (self MetricType) validate() error {
	if int(self) > len(longForm) && int(self) > len(shortForm) {
		return fmt.Errorf("Given MetricType value %d is not valid", self)
	}
	return nil
}

func (self MetricType) String() string {
	if err := self.validate(); err != nil {
		return "unknown"
	}
	return longForm[self]
}

func (self MetricType) shortForm() string {
	if err := self.validate(); err != nil {
		return "unknown"
	}
	return shortForm[self]
}

// More detailed error

type HawkularClientError struct {
	msg  string
	Code int
}

func (self *HawkularClientError) Error() string {
	return fmt.Sprintf("Hawkular returned status code %d, error message: %s", self.Code, self.msg)
}

// Client creation and instance config

type Parameters struct {
	Tenant string
	Port   int
	Host   string
	Path   string
}

type Client struct {
	Tenant  string
	Baseurl string
}

func NewHawkularClient(p Parameters) (*Client, error) {
	url := fmt.Sprintf("http://%s:%d/%s/%s", p.Host, p.Port, p.Path, p.Tenant)
	return &Client{
		Baseurl: url,
		Tenant:  p.Tenant,
	}, nil
}

// Public functions

// Creates a new metric, and returns true if creation succeeded, false if not (metric was already created).
// err is returned only in case of another error than 'metric already created'
func (self *Client) Create(t MetricType, md MetricDefinition) (bool, error) {
	jsonb, err := json.Marshal(&md)
	if err != nil {
		return false, err
	}
	err = self.post(self.metricsUrl(t), jsonb)
	if err, ok := err.(*HawkularClientError); ok {
		if err.Code != http.StatusConflict {
			return false, err
		} else {
			return false, nil
		}
	}
	return true, nil

}

// Take input of single Metric instance. If Timestamp is not defined, use current time
func (self *Client) PushSingleNumericMetric(id string, m Metric) error {

	if _, ok := m.Value.(float64); !ok {
		f, err := ConvertToFloat64(m.Value)
		if err != nil {
			return err
		}
		m.Value = f
	}

	if m.Timestamp == 0 {
		m.Timestamp = UnixMilli(time.Now())
	}

	mH := MetricHeader{Id: id, Data: []Metric{m}}
	return self.WriteMultiple(Numeric, []MetricHeader{mH})
}

func (self *Client) QuerySingleNumericMetric(id string, options map[string]string) ([]Metric, error) {
	return self.query(self.dataUrl(self.singleMetricsUrl(Numeric, id)), options)
}

// func (self *Client) QueryNumericsWithTags(id string, tags map[string]string) ([]MetricDefinition, error) {

// }

func (self *Client) WriteMultiple(metricType MetricType, metrics []MetricHeader) error {
	if err := metricType.validate(); err != nil {
		return err
	}

	jsonb, err := json.Marshal(&metrics)
	if err != nil {
		return err
	}
	return self.post(self.dataUrl(self.metricsUrl(metricType)), jsonb)
}

// Helper functions

// Need tag support here also..

func (self *Client) query(url string, options map[string]string) ([]Metric, error) {
	g, err := self.paramUrl(url, options)
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(g)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return []Metric{}, nil
	} else if resp.StatusCode == http.StatusOK {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		metrics := []Metric{}
		err = json.Unmarshal(b, &metrics)
		if err != nil {
			return nil, err
		}
		return metrics, nil
	} else {
		return nil, self.parseErrorResponse(resp)
	}
}

// func (self *Client) get(url string,

func (self *Client) post(url string, json []byte) error {
	if resp, err := http.Post(url, "application/json", bytes.NewBuffer(json)); err == nil {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			return self.parseErrorResponse(resp)
		}
		return nil
	} else {
		return err
	}
}

func (self *Client) parseErrorResponse(resp *http.Response) error {
	// Parse error messages here correctly..
	reply, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return &HawkularClientError{Code: resp.StatusCode,
			msg: fmt.Sprintf("Reply could not be parsed: %s", err.Error()),
		}
	}

	details := &HawkularError{}

	err = json.Unmarshal(reply, details)
	if err != nil {
		return &HawkularClientError{Code: resp.StatusCode,
			msg: fmt.Sprintf("Reply could not be parsed: %s", err.Error()),
		}
	}

	return &HawkularClientError{Code: resp.StatusCode,
		msg: details.ErrorMsg,
	}
}

func (self *Client) metricType(value interface{}) MetricType {
	if _, ok := value.(float64); ok {
		return Numeric
	} else {
		return Availability
	}
}

// URL functions (...)

func (self *Client) metricsUrl(metricType MetricType) string {
	return fmt.Sprintf("%s/%s", self.Baseurl, metricType.String())
}

func (self *Client) singleMetricsUrl(metricType MetricType, id string) string {
	return fmt.Sprintf("%s/%s", self.metricsUrl(metricType), id)
}

func (self *Client) dataUrl(url string) string {
	return fmt.Sprintf("%s/data", url)
}

func (self *Client) paramUrl(starturl string, options map[string]string) (string, error) {
	u, err := url.Parse(starturl)
	if err != nil {
		return "", err
	}
	q := u.Query()
	for k, v := range options {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// Following methods are to ease the work of the client users

func ConvertToFloat64(v interface{}) (float64, error) {
	switch i := v.(type) {
	case float64:
		return float64(i), nil
	case float32:
		return float64(i), nil
	case int64:
		return float64(i), nil
	case int32:
		return float64(i), nil
	case int16:
		return float64(i), nil
	case int8:
		return float64(i), nil
	case uint64:
		return float64(i), nil
	case uint32:
		return float64(i), nil
	case uint16:
		return float64(i), nil
	case uint8:
		return float64(i), nil
	case int:
		return float64(i), nil
	case uint:
		return float64(i), nil
	case string:
		f, err := strconv.ParseFloat(i, 64)
		if err != nil {
			return math.NaN(), err
		}
		return f, err
	default:
		return math.NaN(), fmt.Errorf("Cannot convert %s to float64", i)
	}
}

// Returns milliseconds since epoch
func UnixMilli(t time.Time) int64 {
	return t.UnixNano() / 1e6
}
