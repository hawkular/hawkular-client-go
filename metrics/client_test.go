package metrics

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"
	assert "github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func integrationClient() (*Client, error) {
	t, err := randomString()
	if err != nil {
		return nil, err
	}
	p := Parameters{Tenant: t, Url: "http://localhost:8080"}
	// p := Parameters{Tenant: t, Host: "localhost:8180"}
	// p := Parameters{Tenant: t, Url: "http://192.168.1.105:8080"}
	// p := Parameters{Tenant: t, Host: "209.132.178.218:18080"}
	return NewHawkularClient(p)
}

func randomString() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%X", b[:]), nil
}

func TestTenant(t *testing.T) {
	c, err := integrationClient()
	assert.NoError(t, err)

	// Create simple Tenant
	id, _ := randomString()
	tenant := TenantDefinition{ID: id}
	created, err := c.CreateTenant(tenant)
	assert.NoError(t, err)
	assert.True(t, created)

	// Create tenant with retention settings
	idr, _ := randomString()
	// var typ MetricType
	typ := Gauge

	retentions := make(map[MetricType]int)
	retentions[typ] = 5
	tenant = TenantDefinition{ID: idr, Retentions: retentions}
	created, err = c.CreateTenant(tenant)
	assert.NoError(t, err)
	assert.True(t, created)

	// Fetch Tenants
	tenants, err := c.Tenants()
	assert.NoError(t, err)
	assert.True(t, len(tenants) > 0)

	var tenantFinder = func(tenantId string, tenantList []*TenantDefinition) *TenantDefinition {
		for _, v := range tenantList {
			if v.ID == tenantId {
				return v
			}
		}
		return nil
	}

	ft := tenantFinder(id, tenants)
	assert.NotNil(t, ft)
	assert.Equal(t, id, ft.ID)

	ft = tenantFinder(idr, tenants)
	assert.NotNil(t, ft)
	assert.Equal(t, idr, ft.ID)

	assert.Equal(t, ft.Retentions[typ], 5)
}

func TestTenantModifier(t *testing.T) {
	c, err := integrationClient()
	assert.Nil(t, err)

	ot, _ := randomString()

	// Create for another tenant
	id := "test.metric.create.numeric.tenant.1"
	md := MetricDefinition{ID: id, Type: Gauge}

	ok, err := c.Create(md, Tenant(ot))
	assert.Nil(t, err)
	assert.True(t, ok, "MetricDefinition should have been created")

	// Try to fetch from default tenant - should fail
	mds, err := c.Definitions(Filters(TypeFilter(Gauge)))
	assert.Nil(t, err)
	assert.Nil(t, mds)

	// Try to fetch from the given tenant - should succeed
	mds, err = c.Definitions(Filters(TypeFilter(Gauge)), Tenant(ot))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mds))
}

func TestCreate(t *testing.T) {
	c, err := integrationClient()
	assert.Nil(t, err)

	id := "test.metric.create.numeric.1"
	md := MetricDefinition{ID: id, Type: Gauge}
	ok, err := c.Create(md)
	assert.Nil(t, err)
	assert.True(t, ok, "MetricDefinition should have been created")

	// Try to recreate the same..
	ok, err = c.Create(md)
	assert.False(t, ok, "Should have received false when recreating them same metric")
	assert.Nil(t, err)

	// Use tags and dataRetention
	tags := make(map[string]string)
	tags["units"] = "bytes"
	tags["env"] = "unittest"
	mdTags := MetricDefinition{ID: "test.metric.create.numeric.2", Tags: tags, Type: Gauge}

	ok, err = c.Create(mdTags)
	assert.True(t, ok, "MetricDefinition should have been created")
	assert.Nil(t, err)

	mdReten := MetricDefinition{ID: "test/metric/create/availability/1", RetentionTime: 12, Type: Availability}
	ok, err = c.Create(mdReten)
	assert.Nil(t, err)
	assert.True(t, ok, "MetricDefinition should have been created")

	// Fetch all the previously created metrics and test equalities..
	mdq, err := c.Definitions(Filters(TypeFilter(Gauge)))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(mdq), "Size of the returned gauge metrics does not match 2")

	mdm := make(map[string]MetricDefinition)
	for _, v := range mdq {
		mdm[v.ID] = *v
	}

	assert.Equal(t, md.ID, mdm[id].ID)
	assert.True(t, reflect.DeepEqual(tags, mdm["test.metric.create.numeric.2"].Tags))

	mda, err := c.Definitions(Filters(TypeFilter(Availability)))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mda))
	assert.Equal(t, "test/metric/create/availability/1", mda[0].ID)
	assert.Equal(t, 12, mda[0].RetentionTime)

	if mda[0].Type != Availability {
		assert.FailNow(t, "Type did not match Availability", mda[0].Type)
	}
}

func TestTagsModification(t *testing.T) {
	c, err := integrationClient()
	assert.Nil(t, err)
	id := "test/tags/modify/1"
	// Create metric without tags
	md := MetricDefinition{ID: id, Type: Gauge}
	ok, err := c.Create(md)
	assert.Nil(t, err)
	assert.True(t, ok, "MetricDefinition should have been created")

	// Add tags
	tags := make(map[string]string)
	tags["ab"] = "ac"
	tags["host"] = "test"
	err = c.UpdateTags(Gauge, id, tags)
	assert.Nil(t, err)

	// Fetch metric tags - check for equality
	mdTags, err := c.Tags(Gauge, id)
	assert.Nil(t, err)

	assert.True(t, reflect.DeepEqual(tags, mdTags), "Tags did not match the updated ones")

	// Delete some metric tags
	err = c.DeleteTags(Gauge, id, tags)
	assert.Nil(t, err)

	// Fetch metric - check that tags were deleted
	mdTags, err = c.Tags(Gauge, id)
	assert.Nil(t, err)
	assert.False(t, len(mdTags) > 0, "Received deleted tags")
}

func TestAddMixedMulti(t *testing.T) {

	// Modify to send both Availability as well as Gauge metrics at the same time
	c, err := integrationClient()
	assert.NoError(t, err)

	startTime := time.Now().Truncate(time.Millisecond)

	mone := Datapoint{Value: 2, Timestamp: startTime}
	hOne := MetricHeader{
		ID:   "test.multi.numeric.1",
		Data: []Datapoint{mone},
		Type: Counter,
	}

	mTwoOne := Datapoint{Value: 1.45, Timestamp: startTime}

	mTwoTwoT := startTime.Add(-1 * time.Second)

	mTwoTwo := Datapoint{Value: float64(4.56), Timestamp: mTwoTwoT}
	hTwo := MetricHeader{
		ID:   "test.multi.numeric.2",
		Data: []Datapoint{mTwoOne, mTwoTwo},
		Type: Gauge,
	}

	h := []MetricHeader{hOne, hTwo}

	err = c.Write(h)
	assert.NoError(t, err)

	var checkDatapoints = func(orig *MetricHeader) {
		metric, err := c.ReadRaw(orig.Type, orig.ID)
		assert.NoError(t, err)
		assert.Equal(t, len(orig.Data), len(metric), "Amount of datapoints does not match expected value")

		for i, d := range metric {
			assert.True(t, orig.Data[i].Timestamp.Equal(d.Timestamp))
			// If the Type was Counter, this is actually an Integer..
			origV, _ := ConvertToFloat64(orig.Data[i].Value)
			recvV, _ := ConvertToFloat64(d.Value)
			assert.Equal(t, origV, recvV)
		}
	}

	checkDatapoints(&hOne)
	checkDatapoints(&hTwo)
}

func TestCheckErrors(t *testing.T) {
	c, err := integrationClient()
	assert.Nil(t, err)

	mH := MetricHeader{
		ID:   "test.number.as.string",
		Data: []Datapoint{Datapoint{Value: "notFloat"}},
		Type: Gauge,
	}

	err = c.Write([]MetricHeader{mH})
	assert.NotNil(t, err, "Invalid non-float value should not be accepted")
	_, err = c.ReadRaw(mH.Type, mH.ID)
	assert.Nil(t, err, "Querying empty metric should not generate an error")
}

func TestTokenAuthenticationWithSSL(t *testing.T) {
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Authorization", r.Header.Get("Authorization"))
	}))
	defer s.Close()

	tenant, err := randomString()
	assert.NoError(t, err)

	tC := &tls.Config{InsecureSkipVerify: true}

	p := Parameters{
		Tenant:    tenant,
		Url:       s.URL,
		Token:     "62590bf9827213afadea8b5077a5bdc0",
		TLSConfig: tC,
	}

	c, err := NewHawkularClient(p)
	assert.NoError(t, err)

	r, err := c.Send(c.Url("GET"))
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("Bearer %s", p.Token), r.Header.Get("X-Authorization"))
}

func TestBuckets(t *testing.T) {
	c, err := integrationClient()
	assert.NoError(t, err)

	tags := make(map[string]string)
	tags["units"] = "bytes"
	tags["env"] = "unittest"
	mdTags := MetricDefinition{ID: "test.buckets.1", Tags: tags, Type: Gauge}

	ok, err := c.Create(mdTags)
	assert.NoError(t, err)
	assert.True(t, ok)

	mone := Datapoint{Value: 1.45, Timestamp: time.Now()}
	hone := MetricHeader{
		ID:   "test.buckets.1",
		Data: []Datapoint{mone},
		Type: Gauge,
	}

	err = c.Write([]MetricHeader{hone})
	assert.NoError(t, err)

	// TODO Muuta PercentilesFilter -> Percentiles (modifier)
	bp, err := c.ReadBuckets(Gauge, Filters(TagsFilter(tags), BucketsFilter(1), PercentilesFilter([]float64{90.0, 99.0})))
	assert.NoError(t, err)
	assert.NotNil(t, bp)

	assert.Equal(t, 1, len(bp))
	assert.Equal(t, int64(1), bp[0].Samples)
	assert.Equal(t, 2, len(bp[0].Percentiles))
	assert.Equal(t, 1.45, bp[0].Percentiles[0].Value)
	assert.Equal(t, 0.9, bp[0].Percentiles[0].Quantile)
	assert.True(t, bp[0].Percentiles[1].Quantile >= 0.99) // Double arithmetic could cause this to be 0.9900000001 etc
}

func TestTagQueries(t *testing.T) {
	c, err := integrationClient()
	assert.NoError(t, err)

	tags := make(map[string]string)

	// Create definitions
	for i := 1; i < 10; i++ {
		hostname := fmt.Sprintf("host%d", i)
		metricID := fmt.Sprintf("test.tags.host.%d", i)
		tags["hostname"] = hostname // No need to worry about using the same map
		md := MetricDefinition{ID: metricID, Tags: tags, Type: Gauge}

		ok, err := c.Create(md)
		assert.NoError(t, err)
		assert.True(t, ok)
	}

	tags["hostname"] = "host[123]"

	// Now query
	mds, err := c.Definitions(Filters(TagsFilter(tags)))
	assert.NoError(t, err)
	assert.Equal(t, 3, len(mds))

	// Now query the available hostnames
	values, err := c.TagValues(tags)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(values))
	assert.Equal(t, 3, len(values["hostname"]))
}
