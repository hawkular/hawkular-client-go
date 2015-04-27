package metrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

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
	client  *http.Client
}

func NewHawkularClient(p Parameters) (*Client, error) {
	url := fmt.Sprintf("http://%s:%d/%s/%s", p.Host, p.Port, p.Path, p.Tenant)
	return &Client{
		Baseurl: url,
		Tenant:  p.Tenant,
		client:  &http.Client{},
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

func (self *Client) QueryMetricDefinitions(t MetricType) ([]*MetricDefinition, error) {
	q := make(map[string]string)
	q["type"] = t.shortForm()
	url, err := self.paramUrl(self.metricsUrl(Generic), q)
	if err != nil {
		return nil, err
	}
	b, err := self.get(url)
	if err != nil {
		return nil, err
	}

	md := []*MetricDefinition{}
	if b != nil {
		if err = json.Unmarshal(b, &md); err != nil {
			return nil, err
		}
	}

	return md, nil
}

func (self *Client) QueryMetricTags(t MetricType, id string) (*map[string]string, error) {
	b, err := self.get(self.tagsUrl(t, id))
	if err != nil {
		return nil, err
	}

	md := MetricDefinition{}
	// Repetive code.. clean up with other queries to somewhere..
	if b != nil {
		if err = json.Unmarshal(b, &md); err != nil {
			return nil, err
		}
	}

	return &md.Tags, nil
}

func (self *Client) UpdateTags(t MetricType, id string, tags map[string]string) error {
	b, err := json.Marshal(tags)
	if err != nil {
		return err
	}
	return self.put(self.tagsUrl(t, id), b)
}

func (self *Client) DeleteTags(t MetricType, id string, deleted map[string]string) error {
	tags := make([]string, 0, len(deleted))
	for k, v := range deleted {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}
	j := strings.Join(tags, ",")
	url := fmt.Sprintf("%s/%s", self.tagsUrl(t, id), j)
	return self.del(url)
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

func (self *Client) QuerySingleNumericMetric(id string, options map[string]string) ([]*Metric, error) {
	url, err := self.paramUrl(self.dataUrl(self.singleMetricsUrl(Numeric, id)), options)
	if err != nil {
		return nil, err
	}
	b, err := self.get(url)
	if err != nil {
		return nil, err
	}
	metrics := []*Metric{}

	if b != nil {
		if err = json.Unmarshal(b, &metrics); err != nil {
			return nil, err
		}
	}

	return metrics, nil

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

func (self *Client) get(url string) ([]byte, error) {
	resp, err := self.client.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		b, err := ioutil.ReadAll(resp.Body)
		return b, err
	} else if resp.StatusCode > 399 {
		return nil, self.parseErrorResponse(resp)
	} else {
		return nil, nil // Nothing to answer..
	}
}

func (self *Client) post(url string, json []byte) error {
	if resp, err := self.client.Post(url, "application/json", bytes.NewBuffer(json)); err == nil {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			return self.parseErrorResponse(resp)
		}
		return nil
	} else {
		return err
	}
}

func (self *Client) put(url string, json []byte) error {
	return self.execHttp(url, "PUT", json)
}

func (self *Client) execHttp(url string, method string, json []byte) error {
	req, _ := http.NewRequest(method, url, bytes.NewReader(json))
	req.Header.Add("Content-Type", "application/json")
	resp, err := self.client.Do(req)

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return self.parseErrorResponse(resp)
	}

	return nil
}

func (self *Client) del(url string) error {
	return self.execHttp(url, "DELETE", nil)
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

func (self *Client) tagsUrl(mt MetricType, id string) string {
	return fmt.Sprintf("%s/tags", self.singleMetricsUrl(mt, id))
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
