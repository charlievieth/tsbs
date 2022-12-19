package intel

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"time"
)

// HostContext contains information needed to create a new host
type HostContext struct {
	id           int
	replicasetId int
	clusterId    int
	groupId      int
	orgId        int
	isPrimary    bool
	start        time.Time
	// used for devops-generic use-case
	metricCount  uint64 // number of metrics to generate
	epochsToLive uint64 // number of epochs to live
}

type intelSimulatorConfig struct {
	// Start is the beginning time for the Simulator
	Start time.Time
	// End is the ending time for the Simulator
	End time.Time
	// InitHostCount is the number of hosts to start with in the first reporting period
	InitHostCount uint64
	// HostCount is the total number of hosts to have in the last reporting period
	HostCount uint64
	// HostConstructor is the function used to create a new Host
	HostConstructor func(ctx *HostContext) Host
	// MaxMetricCount is the max number of metrics per host to create when using generic-devops use-case
	MaxMetricCount uint64
}

func NewHostCtx(id int, replicasetId int, clusterId int, groupId int, orgId int, isPrimary bool, start time.Time) *HostContext {
	return &HostContext{id, replicasetId, clusterId, groupId, orgId, isPrimary, start, 0, 0}
}

func calculateEpochs(c intelSimulatorConfig, interval time.Duration) uint64 {
	return uint64(c.End.Sub(c.Start).Nanoseconds() / interval.Nanoseconds())
}

type commonIntelSimulator struct {
	madePoints uint64
	maxPoints  uint64

	hostIndex       uint64
	clusterIndex    uint64
	replicaSetIndex uint64

	hosts []Host

	epoch      uint64
	epochs     uint64
	epochHosts uint64
	initHosts  uint64

	timestampStart time.Time
	timestampEnd   time.Time
	interval       time.Duration
}

// Finished tells whether we have simulated all the necessary points
func (s *commonIntelSimulator) Finished() bool {
	return s.madePoints >= s.maxPoints
}

func (s *commonIntelSimulator) Fields() map[string][]string {
	if len(s.hosts) <= 0 {
		panic("cannot get fields because no hosts added")
	}
	return s.fields(s.hosts[0].SimulatedMeasurements)
}

func (s *commonIntelSimulator) TagKeys() []string {
	tagKeysAsStr := make([]string, len(MachineTagKeys))
	for i, t := range MachineTagKeys {
		tagKeysAsStr[i] = string(t)
	}
	return tagKeysAsStr
}

func (s *commonIntelSimulator) TagTypes() []string {
	types := make([]string, len(MachineTagKeys))
	for i := 0; i < len(MachineTagKeys); i++ {
		types[i] = machineTagType.String()
	}
	return types
}

func (d *commonIntelSimulator) Headers() *common.GeneratedDataHeaders {
	return &common.GeneratedDataHeaders{
		TagTypes:  d.TagTypes(),
		TagKeys:   d.TagKeys(),
		FieldKeys: d.Fields(),
	}
}
func (s *commonIntelSimulator) fields(measurements []common.SimulatedMeasurement) map[string][]string {
	fields := make(map[string][]string)
	for _, sm := range measurements {
		point := data.NewPoint()
		sm.ToPoint(point)
		fieldKeys := point.FieldKeys()
		fieldKeysAsStr := make([]string, len(fieldKeys))
		for i, k := range fieldKeys {
			fieldKeysAsStr[i] = string(k)
		}
		fields[string(point.MeasurementName())] = fieldKeysAsStr
	}

	return fields
}

func (s *commonIntelSimulator) populatePoint(p *data.Point, measureIdx int) bool {
	host := &s.hosts[s.hostIndex]

	// Populate host-specific tags:
	p.AppendTag(MachineTagKeys[0], host.HostName)
	p.AppendTag(MachineTagKeys[1], host.GroupId)
	p.AppendTag(MachineTagKeys[2], host.OrgId)
	p.AppendTag(MachineTagKeys[3], host.ClusterName)
	p.AppendTag(MachineTagKeys[4], host.ReplicaSetName)
	p.AppendTag(MachineTagKeys[5], host.ReplicaSetState)
	p.AppendTag(MachineTagKeys[6], host.Region)
	p.AppendTag(MachineTagKeys[7], host.Os)
	p.AppendTag(MachineTagKeys[8], host.Arch)
	p.AppendTag(MachineTagKeys[9], host.MongodbVersion)

	// Populate measurement-specific tags and fields:
	host.SimulatedMeasurements[measureIdx].ToPoint(p)

	ret := s.hostIndex < s.epochHosts
	s.madePoints++
	s.hostIndex++
	return ret
}

// TODO(rrk) - Can probably turn this logic into a separate interface and implement other
// types of scale up, e.g., exponential
//
// To "scale up" the number of reporting items, we need to know when
// which epoch we are currently in. Once we know that, we can take the "missing"
// amount of scale -- i.e., the max amount of scale less the initial amount
// -- and add it in proportion to the percentage of epochs that have passed. This
// way we simulate all items at each epoch, but at the end of the function
// we check whether the point should be recorded by the calling process.
func (s *commonIntelSimulator) adjustNumHostsForEpoch() {
	s.epoch++
	missingScale := float64(uint64(len(s.hosts)) - s.initHosts)
	s.epochHosts = s.initHosts + uint64(missingScale*float64(s.epoch)/float64(s.epochs-1))
}
