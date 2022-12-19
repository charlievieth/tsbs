package intel

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math"
	"math/rand"
	"reflect"
	"time"
)

const (
	hostFmt    = "host_%d"
	clusterFmt = "cluster_%d"
	replicaFmt = "replica_%d"
	groupFmt   = "group_%d"
	orgFmt     = "org_%d"
)

var (
	Region = []string{
		"us-east-1",
		"us-west-1",
		"us-west-2",
		"eu-west-1",
		"eu-central-1",
		"ap-southeast-1",
		"ap-southeast-2",
		"ap-northeast-1",
		"sa-east-1",
	}

	MongodbVersion = []string{
		"3.0",
		"3.2",
		"3.4",
		"3.6",
		"4.0",
		"4.2",
		"4.4",
		"5.0",
		"6.0",
		"6.3",
	}

	MachineOSChoices = []string{
		"Ubuntu16.10",
		"Ubuntu16.04LTS",
		"Ubuntu15.10",
		"RHEL7.1",
	}
	MachineArchChoices = []string{
		"x64",
		"x86",
	}
	// no "PRIMARY" or "SECONDARY"
	ReplicaSetStateChoices = []string{
		"ARBITER",
		"DOWN",
		"STARTUP",
		"RECOVERING",
		"STARTUP2",
		"UNKNOWN",
		"REMOVED",
	}

	// MachineTagKeys fields common to all hosts:
	MachineTagKeys = [][]byte{
		[]byte("hostname"),
		[]byte("groupId"),
		[]byte("orgId"),
		[]byte("clusterName"),
		[]byte("replicaSetName"),
		[]byte("replicaSetState"),
		[]byte("region"),
		[]byte("os"),
		[]byte("arch"),
		[]byte("mongodbVersion"),
	}

	// machineTagType is the type of all the tags (string)
	// to be used by TagTypes. Not used elsewhere.
	machineTagType = reflect.TypeOf("some string")
)

// Host models a machine running a single mongodb process
type Host struct {
	SimulatedMeasurements []common.SimulatedMeasurement

	// These are all assigned once, at Host creation:
	HostName        string
	GroupId         string
	OrgId           string
	ClusterName     string
	ReplicaSetName  string
	ReplicaSetState string
	Region          string
	Os              string
	Arch            string
	MongodbVersion  string

	// needed for generic use-cases
	GenericMetricCount uint64 // number of metrics generated
	StartEpoch         uint64
	EpochsToLive       uint64 // 0 means forever

}

type generator func(ctx *HostContext) []common.SimulatedMeasurement

func newIntelHostMeasurements(ctx *HostContext) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		NewIntelMeasurement(ctx.start),
	}
}

func NewHostIntel(ctx *HostContext) Host {
	return newHostWithMeasurementGenerator(newIntelHostMeasurements, ctx)
}

func newHostWithMeasurementGenerator(gen generator, ctx *HostContext) Host {
	sm := gen(ctx)

	src := rand.NewSource(time.Now().UnixNano())
	rand := rand.New(src)
	replicaSetState := "PRIMARY"
	if ctx.isPrimary == false {
		if rand.Float32() > 0.1 {
			replicaSetState = "SECONDARY"
		} else {
			replicaSetState = common.RandomStringSliceChoice(ReplicaSetStateChoices)
		}
	}

	h := Host{
		// Tag Values that are static throughout the life of a Host:
		HostName:        fmt.Sprintf(hostFmt, ctx.id),
		GroupId:         fmt.Sprintf(groupFmt, ctx.groupId),
		OrgId:           fmt.Sprintf(orgFmt, ctx.orgId),
		ClusterName:     fmt.Sprintf(clusterFmt, ctx.clusterId),
		ReplicaSetName:  fmt.Sprintf(replicaFmt, ctx.replicasetId),
		ReplicaSetState: replicaSetState,
		Region:          common.RandomStringSliceChoice(Region),
		Os:              common.RandomStringSliceChoice(MachineOSChoices),
		Arch:            common.RandomStringSliceChoice(MachineArchChoices),
		MongodbVersion:  common.RandomStringSliceChoice(MongodbVersion),

		SimulatedMeasurements: sm,
		GenericMetricCount:    ctx.metricCount,
		StartEpoch:            math.MaxUint64,
		EpochsToLive:          ctx.epochsToLive,
	}

	return h
}

// TickAll advances all Distributions of a Host.
func (h *Host) TickAll(d time.Duration) {
	for i := range h.SimulatedMeasurements {
		h.SimulatedMeasurements[i].Tick(d)
	}
}
