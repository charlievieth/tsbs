package victoriametrics

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/intel"
	"github.com/timescale/tsbs/pkg/query"
	"strings"
	"time"
)

// Intel produces ClickHouse-specific queries for all the Intel query types.
type Intel struct {
	*BaseGenerator
	*intel.Core
}

func (i *Intel) mustGetRandomHosts(nHosts int) []string {
	hosts, err := i.GetRandomHosts(nHosts)
	if err != nil {
		panic(err.Error())
	}
	return hosts
}

func getHostClauseForIntel(hostnames []string) string {
	if len(hostnames) == 0 {
		return ""
	}
	if len(hostnames) == 1 {
		return fmt.Sprintf("hostname='%s'", hostnames[0])
	}
	return fmt.Sprintf("hostname=~'%s'", strings.Join(hostnames, "|"))
}

func (i *Intel) mustGetRandomClusters(nClusters int) []string {
	hosts, err := i.GetRandomClusters(nClusters)
	if err != nil {
		panic(err.Error())
	}
	return hosts
}

func getClusterClauseForIntel(clusternames []string) string {
	if len(clusternames) == 0 {
		return ""
	}
	if len(clusternames) == 1 {
		return fmt.Sprintf("clusterName='%s'", clusternames[0])
	}
	return fmt.Sprintf("clusterName=~'%s'", strings.Join(clusternames, "|"))
}

func getSelectClauseForHosts(metrics, hosts []string) string {
	if len(metrics) == 0 {
		panic("BUG: must be at least one metric name in clause")
	}

	hostsClause := getHostClauseForIntel(hosts)
	if len(metrics) == 1 {

	}

	if len(metrics) == 250 {
		return fmt.Sprintf("{__name__=~'intel_.*', %s}", hostsClause)
	}

	metricsClause := strings.Join(metrics, "|")
	if len(hosts) > 0 {
		return fmt.Sprintf("{__name__=~'intel_(%s)', %s}", metricsClause, hostsClause)
	}
	return fmt.Sprintf("{__name__=~'intel_(%s)'}", metricsClause)
}

func getSelectClauseForClusters(metrics, clusters []string, isPrimary bool) string {
	if len(metrics) == 0 {
		panic("BUG: must be at least one metric name in clause")
	}

	primaryClause := ""
	if isPrimary {
		primaryClause = ", replicaSetState=~'PRIMARY'"
	}

	clustersClause := getClusterClauseForIntel(clusters)
	if len(metrics) == 1 {
		return fmt.Sprintf("intel_%s{%s%s}", metrics[0], clustersClause, primaryClause)
	}

	if len(metrics) == 250 {
		return fmt.Sprintf("{__name__=~'intel_.*', %s}", clustersClause)
	}

	metricsClause := strings.Join(metrics, "|")
	if len(clusters) > 0 {
		return fmt.Sprintf("{__name__=~'intel_(%s)', %s%s}", metricsClause, clustersClause, primaryClause)
	}
	return fmt.Sprintf("{__name__=~'intel_(%s)'}", metricsClause)
}

func mustGetIntelMetricsSlice(numMetrics int) []string {
	metrics, err := intel.GetIntelMetricsSlice(numMetrics)
	if err != nil {
		panic(err.Error())
	}
	return metrics
}

func (i *Intel) AllMetricsForHosts(qi query.Query, nHosts int, duration time.Duration) {
	metrics := mustGetIntelMetricsSlice(250)
	hosts := i.mustGetRandomHosts(nHosts)
	selectClause := getSelectClauseForHosts(metrics, hosts)
	qq := &queryInfo{
		query:    fmt.Sprintf("%s", selectClause),
		label:    fmt.Sprintf("VictoriaMetrics %d intel metric(s), random %4d hosts, duration %s", 250, nHosts, duration),
		interval: i.Interval.MustRandWindow(duration),
		step:     "60",
	}
	i.fillInQuery(qi, qq)
}

func (i *Intel) AllMetricsForClusters(qi query.Query, nClusters int, duration time.Duration) {
	metrics := mustGetIntelMetricsSlice(250)
	clusters := i.mustGetRandomClusters(nClusters)
	selectClause := getSelectClauseForClusters(metrics, clusters, false)
	qq := &queryInfo{
		query:    fmt.Sprintf("%s", selectClause),
		label:    fmt.Sprintf("VictoriaMetrics %d intel metric(s), random %4d clusters, duration %s", 250, nClusters, duration),
		interval: i.Interval.MustRandWindow(duration),
		step:     "60",
	}
	i.fillInQuery(qi, qq)
}

func (i *Intel) HourlyAvgMetricsForHosts(qi query.Query, numMetrics int, nHosts int, duration time.Duration) {
	metrics := mustGetIntelMetricsSlice(numMetrics)
	hosts := i.mustGetRandomHosts(nHosts)
	selectClause := getSelectClauseForHosts(metrics, hosts)
	qq := &queryInfo{
		query:    fmt.Sprintf("avg(avg_over_time(%s[1h])) by (__name__, hostname)", selectClause),
		label:    fmt.Sprintf("VictoriaMetrics hourly avg for %d intel metric(s), random %4d hosts, random %s by 1h", 250, nHosts, duration),
		interval: i.Interval.MustRandWindow(duration),
		step:     "60",
	}
	i.fillInQuery(qi, qq)
}

func (i *Intel) HourlyAvgMetricsForClusters(qi query.Query, numMetrics int, nClusters int, duration time.Duration) {
	metrics := mustGetIntelMetricsSlice(numMetrics)
	clusters := i.mustGetRandomClusters(nClusters)
	selectClause := getSelectClauseForClusters(metrics, clusters, false)
	qq := &queryInfo{
		query:    fmt.Sprintf("avg(avg_over_time(%s[1h])) by (__name__, hostname)", selectClause),
		label:    fmt.Sprintf("VictoriaMetrics hourly avg for %d intel metric(s), random %4d clusters, random %s by 1h", 250, nClusters, duration),
		interval: i.Interval.MustRandWindow(duration),
		step:     "60",
	}
	i.fillInQuery(qi, qq)
}

func (i *Intel) LastPointForHosts(qi query.Query, nHosts int) {
	panic("LastPointForHosts not supported in PromQL")
}

func (i *Intel) LastPointPrimary(qi query.Query) {
	panic("LastPointPrimary not supported in PromQL")
}

func (i *Intel) TopKHostsFromCluster(qi query.Query, nHosts int, duration time.Duration) {
	metrics := mustGetIntelMetricsSlice(1)
	cluster := i.mustGetRandomClusters(1)
	qq := &queryInfo{
		query:    fmt.Sprintf("topk_max(%v, sum(%s) by (hostname), 'clusterName=%s')", nHosts, metrics[0], cluster[0]),
		label:    fmt.Sprintf("VictoriaMetrics top %v hosts for a cluster for %v hours", nHosts, duration.Hours()),
		interval: i.Interval.MustRandWindow(duration),
		step:     "60",
	}
	i.fillInQuery(qi, qq)
}

func (i *Intel) TopKPrimariesFromCluster(qi query.Query, nPrimaries int, duration time.Duration) {
	metrics := mustGetIntelMetricsSlice(1)
	cluster := i.mustGetRandomClusters(1)
	qq := &queryInfo{
		query:    fmt.Sprintf("topk_max(%v, sum(%s{replicaSetState='PRIMARY'}) by (hostname), 'clusterName=%s')", nPrimaries, metrics[0], cluster[0]),
		label:    fmt.Sprintf("VictoriaMetrics top %v primaries for a cluster for %v hours", nPrimaries, duration.Hours()),
		interval: i.Interval.MustRandWindow(duration),
		step:     "60",
	}
	i.fillInQuery(qi, qq)
}

func (i *Intel) CounterRateHost(qi query.Query, nHosts int, duration time.Duration) {
	metrics := []string{"intel_mongodb_asserts_regular"}
	hosts := i.mustGetRandomHosts(nHosts)
	selectClause := getSelectClauseForHosts(metrics, hosts)
	qq := &queryInfo{
		query:    fmt.Sprintf("sum(rate(%s[5m])) by (__name__, hostname)", selectClause),
		label:    fmt.Sprintf("VictoriaMetrics rate for counter metric, random %4d hosts, random dureation %s by 5m", nHosts, duration),
		interval: i.Interval.MustRandWindow(duration),
		step:     "60",
	}
	i.fillInQuery(qi, qq)
}
