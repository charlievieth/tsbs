package mongo

import (
	"encoding/gob"
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/intel"
	"github.com/timescale/tsbs/pkg/query"
	"go.mongodb.org/mongo-driver/bson"
	"time"
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

func (i *Intel) TopKHostsFromCluster(qi query.Query, duration time.Duration) {
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
					"output": bson.A{
						"$tags.hostname",
						"$mongodb_extra_info_user_time_us",
					},
					"sortBy": bson.M{
						"mongodb_extra_info_user_time_us": -1,
					},
					"n": 10,
				},
			},
		}},
	}

	label := fmt.Sprintf("Mongo top 10 hosts for a cluster for %v hours", duration.Hours())
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(label)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s: start %s", label, interval.StartString()))
}
