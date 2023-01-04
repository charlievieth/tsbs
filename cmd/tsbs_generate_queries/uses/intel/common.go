package intel

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/pkg/query"
	"time"
)

const (
	// LabelAllMetricsForHosts is the label prefix for queries
	LabelAllMetricsForHosts = "all-metrics-host"
	LabelLastPointPrimary   = "last-point-primary-host"
	LabelLastPointForHosts  = "last-point-host"
	LabelTopKForCluster     = "topk-node"
)

// Core is the common component of all generators for all systems.
type Core struct {
	*common.Core
}

// NewCore returns a new Core for the given time range and cardinality
func NewCore(start, end time.Time, scale int) (*Core, error) {
	c, err := common.NewCore(start, end, scale)
	return &Core{Core: c}, err
}

// 100 clusters of 30 shards, 3 hosts each
// 100 * 30 * 3 = 9000

// GetRandomHosts returns a random set of nHosts from a given Core
func (d *Core) GetRandomHosts(nHosts int) ([]string, error) {
	return getRandomNameOfMetadataType(nHosts, 9000, Host)
}

// GetRandomClusters returns a random set of nClusters from a given Core
func (d *Core) GetRandomClusters(nClusters int) ([]string, error) {
	return getRandomNameOfMetadataType(nClusters, 100, Cluster)
}

// GetRandomReplicas returns a random set of nReplicas from a given Core
func (d *Core) GetRandomReplicas(nReplicas int) ([]string, error) {
	return getRandomNameOfMetadataType(nReplicas, 30, Replica)
}

// GetRandomGroups returns a random set of nGroups from a given Core
func (d *Core) GetRandomGroups(nGroups int) ([]string, error) {
	return getRandomNameOfMetadataType(nGroups, 100, Group)
}

// GetRandomOrgs returns a random set of nOrgs from a given Core
func (d *Core) GetRandomOrgs(nOrgs int) ([]string, error) {
	return getRandomNameOfMetadataType(nOrgs, 100, Org)
}

type AllMetricsFiller interface {
	AllMetricsForHosts(query.Query, int, time.Duration)
}

type TopKHostsFromClusterFiller interface {
	TopKHostsFromCluster(query.Query, int, time.Duration)
}

type LastPointFiller interface {
	LastPointPrimary(query.Query)
}

type LastPointForHostsFiller interface {
	LastPointForHosts(query.Query, int)
}

type MetadataType string

const (
	Host    MetadataType = "host"
	Cluster              = "cluster"
	Replica              = "replica"
	Group                = "group"
	Org                  = "org"
)

// getRandomNameOfMetadataType returns a subset of num names of a permutation of names of a particular type,
// numbered from 0 to total.
// Ex.: if type is host, host_12, host_7, host_25 for numH=3 and total=30 (3 out of 30)
func getRandomNameOfMetadataType(num int, total int, metadataType MetadataType) ([]string, error) {
	if num < 1 {
		return nil, fmt.Errorf("num cannot be < 1; got %d", num)
	}
	if num > total {
		return nil, fmt.Errorf("num (%d) larger than total. See --scale (%d)", num, total)
	}

	randomNumbers, err := common.GetRandomSubsetPerm(num, total)
	if err != nil {
		return nil, err
	}

	names := []string{}
	for _, n := range randomNumbers {
		names = append(names, fmt.Sprintf("%v_%d", metadataType, n))
	}

	return names, nil
}
