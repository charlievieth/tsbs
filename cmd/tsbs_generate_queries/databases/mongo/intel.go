package mongo

import (
	"encoding/gob"
	"fmt"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/intel"
	"github.com/timescale/tsbs/pkg/query"
	"go.mongodb.org/mongo-driver/bson"
)

func init() {
	// needed for serializing the mongo query to gob
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register([]map[string]interface{}{})
	gob.Register(bson.M{})
	gob.Register(bson.D{})
	gob.Register([]bson.M{})
	gob.Register(time.Time{})
}

// const ()

// Intel produces Mongo-specific queries for the Intel use case.
type Intel struct {
	*BaseGenerator
	*intel.Core
}

func (i *Intel) AllMetricsForHosts(qi query.Query, nHosts int, duration time.Duration) {
	interval := i.Interval.MustRandWindow(duration)
	hostnames, err := i.GetRandomHosts(nHosts)
	panicIfErr(err)

	pipelineQuery := []bson.M{
		{
			"$match": bson.M{
				"tags.hostname": bson.M{
					"$in": hostnames,
				},
				"time": bson.M{
					"$gte": interval.Start(),
					"$lt":  interval.End(),
				},
			},
		},
	}

	label := fmt.Sprintf("Mongo all metrics for %v hosts for %v hours", nHosts, duration.Hours())
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(label)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: start %s", label, interval.StartString()))
}

func (i *Intel) AllMetricsForClusters(qi query.Query, nClusters int, duration time.Duration) {
	interval := i.Interval.MustRandWindow(duration)
	clusterNames, err := i.GetRandomClusters(nClusters)
	panicIfErr(err)

	pipelineQuery := []bson.M{
		{
			"$match": bson.M{
				"tags.clusterName": bson.M{
					"$in": clusterNames,
				},
				"time": bson.M{
					"$gte": interval.Start(),
					"$lt":  interval.End(),
				},
			},
		},
	}

	label := fmt.Sprintf("Mongo all metrics for %v clusters for %v hours", nClusters, duration.Hours())
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(label)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: start %s", label, interval.StartString()))
}

func (i *Intel) HourlyAvgMetricsForHosts(qi query.Query, numMetrics int, nHosts int, duration time.Duration) {
	interval := i.Interval.MustRandWindow(duration)
	metrics, err := intel.GetIntelMetricsSlice(numMetrics)
	panicIfErr(err)
	hostnames, err := i.GetRandomHosts(nHosts)
	panicIfErr(err)

	pipelineQuery := []bson.M{
		{
			"$match": bson.M{
				"tags.hostname": bson.M{
					"$in": hostnames,
				},
				"time": bson.M{
					"$gte": interval.Start(),
					"$lt":  interval.End(),
				},
			},
		},
		{
			"$group": bson.M{
				"_id": bson.M{
					"time": bson.M{
						"$dateTrunc": bson.M{"date": "$time", "unit": "hour"},
					},
					"hostname": "$tags.hostname",
				},
			},
		},
		{
			"$sort": bson.D{{"_id.time", 1}, {"_id.hostname", 1}},
		},
	}
	resultMap := pipelineQuery[1]["$group"].(bson.M)
	for _, metric := range metrics {
		resultMap["avg_"+metric] = bson.M{"$avg": "$" + metric}
	}

	humanLabel := fmt.Sprintf("Mongo mean of %d metrics %v hosts for %v hours", numMetrics, nHosts, duration.Hours())
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s (%s)", humanLabel, interval.StartString(), q.CollectionName))
}

func (i *Intel) HourlyAvgMetricsForClusters(qi query.Query, numMetrics int, nClusters int, duration time.Duration) {
	interval := i.Interval.MustRandWindow(duration)
	metrics, err := intel.GetIntelMetricsSlice(numMetrics)
	panicIfErr(err)
	clusterNames, err := i.GetRandomClusters(nClusters)
	panicIfErr(err)

	pipelineQuery := []bson.M{
		{
			"$match": bson.M{
				"tags.clusterName": bson.M{
					"$in": clusterNames,
				},
				"time": bson.M{
					"$gte": interval.Start(),
					"$lt":  interval.End(),
				},
			},
		},
		{
			"$group": bson.M{
				"_id": bson.M{
					"time": bson.M{
						"$dateTrunc": bson.M{"date": "$time", "unit": "hour"},
					},
					"hostname": "$tags.hostname",
				},
			},
		},
		{
			"$sort": bson.D{{"_id.time", 1}, {"_id.hostname", 1}},
		},
	}
	resultMap := pipelineQuery[1]["$group"].(bson.M)
	for _, metric := range metrics {
		resultMap["avg_"+metric] = bson.M{"$avg": "$" + metric}
	}

	humanLabel := fmt.Sprintf("Mongo mean of %d metrics %v clusters for %v hours", numMetrics, nClusters, duration.Hours())
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s (%s)", humanLabel, interval.StartString(), q.CollectionName))
}

func (i *Intel) LastPointPrimary(qi query.Query) {
	pipelineQuery := []bson.M{
		{"$sort": bson.M{
			"tags.hostname": 1,
			"time":          -1,
		}},
		{"$match": bson.M{
			"tags.replicaSetState": "PRIMARY",
		}},
		{"$group": bson.M{
			"_id": "$tags.hostname",
			"ts": bson.M{
				"$first": "$time",
			},
			"mongodb_asserts_rollovers": bson.M{
				"$first": "$mongodb_asserts_rollovers",
			},
		}},
	}

	humanLabel := "Mongo last metric per primary host"
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s", humanLabel))
}

func (i *Intel) LastPointForHosts(qi query.Query, nHosts int) {
	hostnames, err := i.GetRandomHosts(nHosts)
	panicIfErr(err)

	pipelineQuery := []bson.M{
		{"$match": bson.M{
			"tags.hostname": bson.M{
				"$in": hostnames,
			},
		}},
		{"$sort": bson.M{
			"tags.hostname": 1,
			"time":          -1,
		}},
		{"$group": bson.M{
			"_id": "$tags.hostname",
			"ts": bson.M{
				"$first": "$time",
			},
			"mongodb_asserts_rollovers": bson.M{
				"$first": "$mongodb_asserts_rollovers",
			},
		}},
	}

	humanLabel := fmt.Sprintf("Mongo last metric for %v hosts", nHosts)
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s", humanLabel))
}

func (i *Intel) TopKHostsFromCluster(qi query.Query, nHosts int, duration time.Duration) {
	interval := i.Interval.MustRandWindow(duration)
	clusterNames, err := i.GetRandomClusters(1)
	panicIfErr(err)
	pipelineQuery := []bson.M{
		{
			"$match": bson.M{
				"tags.clusterName": bson.M{
					"$in": clusterNames,
				},
				"time": bson.M{
					"$gte": interval.Start(),
					"$lt":  interval.End(),
				},
			},
		},
		{"$group": bson.M{
			"_id": "$tags.clusterName",
			"topHosts": bson.M{
				"$topN": bson.M{
					"output": []string{"$tags.hostname", "$mongodb_extra_info_user_time_us"},
					"sortBy": bson.M{
						"mongodb_extra_info_user_time_us": -1,
					},
					"n": nHosts,
				},
			},
		}},
	}

	label := fmt.Sprintf("Mongo top %v hosts for a cluster for %v hours", nHosts, duration.Hours())
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(label)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: start %s", label, interval.StartString()))
}

func (i *Intel) TopKPrimariesFromCluster(qi query.Query, nPrimaries int, duration time.Duration) {
	interval := i.Interval.MustRandWindow(duration)
	clusterNames, err := i.GetRandomClusters(1)
	panicIfErr(err)
	pipelineQuery := []bson.M{
		{
			"$match": bson.M{
				"tags.clusterName": bson.M{
					"$in": clusterNames,
				},
				"time": bson.M{
					"$gte": interval.Start(),
					"$lt":  interval.End(),
				},
				"tags.replicaSetState": "PRIMARY",
			},
		},
		{"$group": bson.M{
			"_id": "$tags.clusterName",
			"topHosts": bson.M{
				"$topN": bson.M{
					"output": []string{"$tags.hostname", "$mongodb_extra_info_user_time_us"},
					"sortBy": bson.M{
						"mongodb_extra_info_user_time_us": -1,
					},
					"n": nPrimaries,
				},
			},
		}},
	}

	label := fmt.Sprintf("Mongo top %v primaries for a cluster for %v hours", nPrimaries, duration.Hours())
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(label)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: start %s", label, interval.StartString()))
}
