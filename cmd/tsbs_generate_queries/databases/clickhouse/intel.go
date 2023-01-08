package clickhouse

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

func (i *Intel) getHostWhereWithClusternames(clusterNames []string) string {
	clusterNameSelectionClauses := []string{}

	if i.UseTags {
		// Use separated table for Tags
		// Need to prepare WHERE with `tags` table
		// WHERE tags_id IN (SELECT those tag.id FROM separated tags table WHERE )
		for _, s := range clusterNames {
			clusterNameSelectionClauses = append(clusterNameSelectionClauses, fmt.Sprintf("'%s'", s))
		}
		return fmt.Sprintf("tags_id IN (SELECT id FROM tags WHERE clusterName IN (%s))", strings.Join(clusterNameSelectionClauses, ","))
	}

	// Here we DO NOT use tags as a separate table
	// So hostname is embedded into processed table itself, and we can build direct WHERE statement as
	// ((clusterName = 'H1') OR (clusterName = 'H2') ...)

	// All tags are included into one table
	// Need to prepare WHERE (clusterName = 'cluster1' OR clusterName = 'cluster2') clause
	for _, s := range clusterNames {
		clusterNameSelectionClauses = append(clusterNameSelectionClauses, fmt.Sprintf("clusterName = '%s'", s))
	}

	return "(" + strings.Join(clusterNameSelectionClauses, " OR ") + ")"
}

func (i *Intel) getClusterWhereString(nclusters int) string {
	clusterNames, err := i.GetRandomClusters(nclusters)
	panicIfErr(err)
	return i.getHostWhereWithClusternames(clusterNames)
}

// getHostWhereWithHostnames creates WHERE SQL statement for multiple hostnames.
// NOTE: 'WHERE' itself is not included, just hostname filter clauses, ready to concatenate to 'WHERE' string
func (i *Intel) getHostWhereWithHostnames(hostnames []string) string {
	hostnameSelectionClauses := []string{}

	if i.UseTags {
		// Use separated table for Tags
		// Need to prepare WHERE with `tags` table
		// WHERE tags_id IN (SELECT those tag.id FROM separated tags table WHERE )
		for _, s := range hostnames {
			hostnameSelectionClauses = append(hostnameSelectionClauses, fmt.Sprintf("'%s'", s))
		}
		return fmt.Sprintf("tags_id IN (SELECT id FROM tags WHERE hostname IN (%s))", strings.Join(hostnameSelectionClauses, ","))
	}

	// Here we DO NOT use tags as a separate table
	// So hostname is embedded into processed table itself and we can build direct WHERE statement as
	// ((hostname = 'H1') OR (hostname = 'H2') ...)

	// All tags are included into one table
	// Need to prepare WHERE (hostname = 'host1' OR hostname = 'host2') clause
	for _, s := range hostnames {
		hostnameSelectionClauses = append(hostnameSelectionClauses, fmt.Sprintf("hostname = '%s'", s))
	}
	// (host=h1 OR host=h2)
	return "(" + strings.Join(hostnameSelectionClauses, " OR ") + ")"
}

// getHostWhereString gets multiple random hostnames and create WHERE SQL statement for these hostnames.
func (i *Intel) getHostWhereString(nhosts int) string {
	hostnames, err := i.GetRandomHosts(nhosts)
	panicIfErr(err)
	return i.getHostWhereWithHostnames(hostnames)
}

// getSelectClausesAggMetrics gets specified aggregate function clause for multiple metrics
// Ex.: max(metric_1) AS max_metric_1
func (i *Intel) getSelectClausesAggMetrics(aggregateFunction string, metrics []string) []string {
	selectAggregateClauses := make([]string, len(metrics))
	for i, metric := range metrics {
		selectAggregateClauses[i] = fmt.Sprintf("%[1]s(%[2]s) AS %[1]s_%[2]s", aggregateFunction, metric)
	}
	return selectAggregateClauses
}

func (i *Intel) AllMetricsForHosts(qi query.Query, nHosts int, duration time.Duration) {
	interval := i.Interval.MustRandWindow(duration)

	sql := fmt.Sprintf(`
			SELECT *
			FROM intel
			WHERE %s AND (created_at >= '%s') AND (created_at < '%s')
        `,
		i.getHostWhereString(nHosts),
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat))

	humanLabel := fmt.Sprintf("ClickHouse all intel metric(s) for random %4d hosts, duration: %s", nHosts, duration)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	i.fillInQuery(qi, humanLabel, humanDesc, "intel", sql)
}

func (i *Intel) HourlyAvgMetricsForHosts(qi query.Query, numMetrics int, nHosts int, duration time.Duration) {
	interval := i.Interval.MustRandWindow(duration)
	metrics, err := intel.GetIntelMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := i.getSelectClausesAggMetrics("avg", metrics)

	sql := fmt.Sprintf(`
        SELECT
            toStartOfHour(created_at) AS hour,
            %s
        FROM intel
        WHERE %s AND (created_at >= '%s') AND (created_at < '%s')
        GROUP BY hour
        ORDER BY hour ASC
        `,
		strings.Join(selectClauses, ", "),
		i.getHostWhereString(nHosts),
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat))

	humanLabel := fmt.Sprintf("ClickHouse %d intel metric(s), random %4d hosts, random %s by 1h", numMetrics, nHosts, duration)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	i.fillInQuery(qi, humanLabel, humanDesc, "intel", sql)
}

func (i *Intel) LastPointForHosts(qi query.Query, nHosts int) {
	sql := fmt.Sprintf(`SELECT *
            FROM
            (
                SELECT *
                FROM intel
                WHERE (tags_id, created_at) IN
                (
                    SELECT
                        tags_id,
                        max(created_at)
                    FROM intel
					WHERE %s
                    GROUP BY tags_id
                )
            ) AS c
            ANY INNER JOIN tags AS t ON c.tags_id = t.id 
            WHERE t.replicaSetState = 'PRIMARY'
            ORDER BY
                t.hostname ASC,
                c.time DESC`, i.getHostWhereString(nHosts))

	humanLabel := fmt.Sprintf("ClickHouse last point for %v hosts", nHosts)
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, "intel", sql)
}

func (i *Intel) LastPointPrimary(qi query.Query) {
	sql := `SELECT *
            FROM
            (
                SELECT *
                FROM intel
                WHERE (tags_id, created_at) IN
                (
                    SELECT
                        tags_id,
                        max(created_at)
                    FROM intel
                    GROUP BY tags_id
                )
            ) AS c
            ANY INNER JOIN tags AS t ON c.tags_id = t.id 
            WHERE t.replicaSetState = 'PRIMARY'
            ORDER BY
                t.hostname ASC,
                c.time DESC`

	humanLabel := "ClickHouse last point per primary host"
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, "intel", sql)
}

func (i *Intel) TopKHostsFromCluster(qi query.Query, nHosts int, duration time.Duration) {
	interval := i.Interval.MustRandWindow(duration)
	sql := fmt.Sprintf(`SELECT 
				max(intel.mongodb_extra_info_user_time_us) as max_mongodb_extra_info_user_time_us,
				t.hostname
			FROM intel
            ANY INNER JOIN tags AS t ON intel.tags_id = t.id 
			WHERE intel.tags_id IN (%s)
            AND (created_at >= '%s') AND (created_at < '%s')
            GROUP BY t.hostname
            ORDER BY max_mongodb_extra_info_user_time_us DESC
            LIMIT %v`,
		i.getClusterWhereString(1),
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat),
		nHosts)
	humanLabel := fmt.Sprintf("Clickhouse top %v hosts for a cluster for %v hours", nHosts, duration.Hours())
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, "intel", sql)
}

func (i *Intel) TopKPrimariesFromCluster(qi query.Query, nPrimaries int, duration time.Duration) {
	interval := i.Interval.MustRandWindow(duration)
	sql := fmt.Sprintf(`SELECT 
				max(intel.mongodb_extra_info_user_time_us) as max_mongodb_extra_info_user_time_us,
				t.hostname
			FROM intel
            ANY INNER JOIN tags AS t ON intel.tags_id = t.id 
			WHERE intel.tags_id IN (%s)
            AND (created_at >= '%s') AND (created_at < '%s')
			AND tags.replicaSetState = 'PRIMARY'
            GROUP BY t.hostname
            ORDER BY max_mongodb_extra_info_user_time_us DESC
            LIMIT %v`,
		i.getClusterWhereString(1),
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat),
		nPrimaries)
	humanLabel := fmt.Sprintf("Clickhouse top %v hosts for a cluster for %v hours", nPrimaries, duration.Hours())
	humanDesc := humanLabel
	i.fillInQuery(qi, humanLabel, humanDesc, "intel", sql)
}
