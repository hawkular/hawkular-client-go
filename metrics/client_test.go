package metrics

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"
)

func integrationClient() (*Client, error) {
	t, err := randomString()
	if err != nil {
		return nil, err
	}
	p := Parameters{Tenant: t, Port: 8080, Host: "localhost", Path: "hawkular/metrics"}
	// p := Parameters{Tenant: t, Port: 18080, Host: "209.132.178.218"}
	return NewHawkularClient(p)
}

func randomString() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%X", b[:]), nil
}

func createError(err error) {
}

func TestCreate(t *testing.T) {
	c, err := integrationClient()
	if err != nil {
		t.Error(err.Error())
	}

	md := MetricDefinition{Id: "test.metric.create.numeric.1"}
	if _, err = c.Create(Numeric, md); err != nil {
		t.Error(err.Error())
	}

	// Try to recreate the same..
	ok, err := c.Create(Numeric, md)
	if ok {
		t.Error("Should have received fail when recreating them same metric")
	}
	if err != nil {
		t.Errorf("Could not parse error reply from Hawkular, %s", err.Error())
	}

	// Use tags and dataRetention

	tags := make(map[string]string)
	tags["units"] = "bytes"
	tags["env"] = "unittest"
	md_tags := MetricDefinition{Id: "test.metric.create.numeric.2", Tags: tags}
	if _, err = c.Create(Numeric, md_tags); err != nil {
		t.Errorf(err.Error())
	}

	md_reten := MetricDefinition{Id: "test.metric.create.availability.1", RetentionTime: 12}
	if _, err = c.Create(Availability, md_reten); err != nil {
		t.Errorf(err.Error())
	}

}

func TestAddNumericSingle(t *testing.T) {
	c, err := integrationClient()
	if err != nil {
		t.Error(err.Error())
	}

	// With timestamp
	m := Metric{Timestamp: time.Now().UnixNano() / 1e6, Value: 1.34}
	if err = c.PushSingleNumericMetric("test.numeric.single.1", m); err != nil {
		t.Error(err.Error())
	}

	// Without preset timestamp
	m = Metric{Value: 2}
	if err = c.PushSingleNumericMetric("test.numeric.single.2", m); err != nil {
		t.Error(err.Error())
	}

	// Query for both metrics and check that they're correctly filled
	params := make(map[string]string)
	metrics, err := c.QuerySingleNumericMetric("test.numeric.single.1", params)
	if err != nil {
		t.Error(err)
	}

	if len(metrics) != 1 {
		t.Errorf("Received %d metrics instead of 1", len(metrics))
	}

	metrics, err = c.QuerySingleNumericMetric("test.numeric.single.2", params)

	if len(metrics) != 1 {
		t.Errorf("Received %d metrics instead of 1", len(metrics))
	} else {
		if metrics[0].Timestamp < 1 {
			t.Error("Timestamp was not correctly populated")
		}
	}

}

// func TestTagsModification(t *testing.T) {
// 	if c, err := integrationClient(); err == nil {
// 		// Create metric without tags

// 		// Add tags

// 		// Fetch metric definition - check for tags

// 		// Delete some metric tags

// 		// Fetch metric - check that tags were deleted
// 	}
// }

func TestTags(t *testing.T) {
	if c, err := integrationClient(); err == nil {
		tags := make(map[string]string)
		tTag, err := randomString()
		tags[tTag] = "testValue"

		// Write with tags
		m := Metric{Value: float64(0.01), Tags: tags}
		err = c.PushSingleNumericMetric("test.tags.numeric.1", m)
		if err != nil {
			t.Error(err)
		}

		// Search metrics with tag

		// 		    @GET
		// @Path("/{tenantId}/numeric")
		// @ApiOperation(value = "Find numeric metrics data by their tags.", response = Map.cla@ApiParam(value = "Tag list", required = true) @QueryParam("tags") Tags tags

		// Get metric definition tags
		// @Path("/{tenantId}/metrics/numeric/{id}/tags")
		// @ApiOperation(value = "Retrieve tags associated with the metric definition.", response = Metric.class)

		// Fetch a metric with values and check we still have tags
	}
}

func TestAddMixedMulti(t *testing.T) {

	// Modify to send both Availability as well as Gauge metrics at the same time
	if c, err := integrationClient(); err == nil {

		mone := Metric{Value: 1.45, Timestamp: UnixMilli(time.Now())}
		hone := MetricHeader{Id: "test.multi.numeric.1",
			Data: []Metric{mone}}

		mtwo_1 := Metric{Value: 2, Timestamp: UnixMilli(time.Now())}

		mtwo_2_t := UnixMilli(time.Now()) - 1e3

		mtwo_2 := Metric{Value: float64(4.56), Timestamp: mtwo_2_t}
		htwo := MetricHeader{Id: "test.multi.numeric.2", Data: []Metric{mtwo_1, mtwo_2}}

		h := []MetricHeader{hone, htwo}

		err = c.WriteMultiple(Numeric, h)
		if err != nil {
			t.Error(err)
		}

		var getMetric = func(id string) []Metric {
			metric, err := c.QuerySingleNumericMetric(id, make(map[string]string))
			if err != nil {
				t.Error(err)
			}
			return metric
		}

		m := getMetric("test.multi.numeric.1")
		if len(m) != 1 {
			t.Errorf("Received %d metrics instead of 1", len(m))
		}

		m = getMetric("test.multi.numeric.2")
		if len(m) != 2 {
			t.Errorf("Received %d metrics, expected 2", len(m))
		}
	} else {
		t.Error(err)
	}
}

func TestCheckErrors(t *testing.T) {
	c, err := integrationClient()
	if err != nil {
		t.Fail()
	}

	if err = c.PushSingleNumericMetric("test.number.as.string", Metric{Value: "notFloat"}); err == nil {
		t.Fail()
	}

	if _, err = c.QuerySingleNumericMetric("test.not.existing", make(map[string]string)); err != nil {
		t.Error("Not existing should not generate an error")
	}
}
