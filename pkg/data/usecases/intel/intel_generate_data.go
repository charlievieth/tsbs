package intel

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"time"
)

// It fulfills the Simulator interface.
type IntelSimulator struct {
	*commonIntelSimulator
}

// Fields returns a map of subsystems to metrics collected
func (d *IntelSimulator) Fields() map[string][]string {
	return d.fields(d.hosts[0].SimulatedMeasurements[:1])
}

func (d *IntelSimulator) Headers() *common.GeneratedDataHeaders {
	return &common.GeneratedDataHeaders{
		TagTypes:  d.TagTypes(),
		TagKeys:   d.TagKeys(),
		FieldKeys: d.Fields(),
	}
}

// Next advances a Point to the next state in the generator.
func (d *IntelSimulator) Next(p *data.Point) bool {
	// Switch to the next metric if needed
	if d.hostIndex == uint64(len(d.hosts)) {
		d.hostIndex = 0

		for i := 0; i < len(d.hosts); i++ {
			d.hosts[i].TickAll(d.interval)
		}

		d.adjustNumHostsForEpoch()
	}

	return d.populatePoint(p, 0)
}

type IntelSimulatorConfig intelSimulatorConfig

// NewSimulator produces a Simulator that conforms to the given SimulatorConfig over the specified interval
func (c *IntelSimulatorConfig) NewSimulator(interval time.Duration, limit uint64) common.Simulator {
	// 100 clusters of 30 shards, 3 hosts each
	// 100 * 30 * 3 = 9000
	clusterCount := 100
	shardCount := 30
	replicaCount := 3

	var hostInfos []Host

	hostId := 0
	for clusterSuffix := 0; clusterSuffix < clusterCount; clusterSuffix++ {
		for replicaSetSuffix := 0; replicaSetSuffix < shardCount; replicaSetSuffix++ {
			isPrimary := true
			for replica := 0; replica < replicaCount; replica++ {
				// use clusterSuffix for group and org id suffix too
				hostInfos = append(hostInfos, c.HostConstructor(NewHostCtx(hostId, replicaSetSuffix, clusterSuffix, clusterSuffix, clusterSuffix, isPrimary, c.Start)))
				hostId++
				isPrimary = false
			}
		}
	}

	epochs := calculateEpochs(intelSimulatorConfig(*c), interval)
	maxPoints := epochs * c.HostCount
	if limit > 0 && limit < maxPoints {
		// Set specified points number limit
		maxPoints = limit
	}
	sim := &IntelSimulator{
		commonIntelSimulator: &commonIntelSimulator{
			madePoints: 0,
			maxPoints:  maxPoints,

			hostIndex: 0,
			hosts:     hostInfos,

			epoch:          0,
			epochs:         epochs,
			epochHosts:     c.InitHostCount,
			initHosts:      c.InitHostCount,
			timestampStart: c.Start,
			timestampEnd:   c.End,
			interval:       interval,
		},
	}

	return sim
}
