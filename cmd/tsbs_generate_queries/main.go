// tsbs_generate_queries generates queries for various use cases. Its output will
// be consumed by the corresponding tsbs_run_queries_ program.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/intel"
	"github.com/timescale/tsbs/pkg/query/config"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/internal/inputs"
	internalUtils "github.com/timescale/tsbs/internal/utils"
)

var useCaseMatrix = map[string]map[string]utils.QueryFillerMaker{
	"devops": {
		devops.LabelSingleGroupby + "-1-1-1":  devops.NewSingleGroupby(1, 1, 1),
		devops.LabelSingleGroupby + "-1-1-12": devops.NewSingleGroupby(1, 1, 12),
		devops.LabelSingleGroupby + "-1-8-1":  devops.NewSingleGroupby(1, 8, 1),
		devops.LabelSingleGroupby + "-5-1-1":  devops.NewSingleGroupby(5, 1, 1),
		devops.LabelSingleGroupby + "-5-1-12": devops.NewSingleGroupby(5, 1, 12),
		devops.LabelSingleGroupby + "-5-8-1":  devops.NewSingleGroupby(5, 8, 1),
		devops.LabelMaxAll + "-1":             devops.NewMaxAllCPU(1, devops.MaxAllDuration),
		devops.LabelMaxAll + "-8":             devops.NewMaxAllCPU(8, devops.MaxAllDuration),
		devops.LabelMaxAll + "-32-24":         devops.NewMaxAllCPU(32, 24*time.Hour),
		devops.LabelDoubleGroupby + "-1":      devops.NewGroupBy(1),
		devops.LabelDoubleGroupby + "-5":      devops.NewGroupBy(5),
		devops.LabelDoubleGroupby + "-all":    devops.NewGroupBy(devops.GetCPUMetricsLen()),
		devops.LabelGroupbyOrderbyLimit:       devops.NewGroupByOrderByLimit,
		devops.LabelHighCPU + "-all":          devops.NewHighCPU(0),
		devops.LabelHighCPU + "-1":            devops.NewHighCPU(1),
		devops.LabelLastpoint:                 devops.NewLastPointPerHost,
	},
	"iot": {
		iot.LabelLastLoc:                       iot.NewLastLocPerTruck,
		iot.LabelLastLocSingleTruck:            iot.NewLastLocSingleTruck,
		iot.LabelLowFuel:                       iot.NewTruckWithLowFuel,
		iot.LabelHighLoad:                      iot.NewTruckWithHighLoad,
		iot.LabelStationaryTrucks:              iot.NewStationaryTrucks,
		iot.LabelLongDrivingSessions:           iot.NewTrucksWithLongDrivingSession,
		iot.LabelLongDailySessions:             iot.NewTruckWithLongDailySession,
		iot.LabelAvgVsProjectedFuelConsumption: iot.NewAvgVsProjectedFuelConsumption,
		iot.LabelAvgDailyDrivingDuration:       iot.NewAvgDailyDrivingDuration,
		iot.LabelAvgDailyDrivingSession:        iot.NewAvgDailyDrivingSession,
		iot.LabelAvgLoad:                       iot.NewAvgLoad,
		iot.LabelDailyActivity:                 iot.NewDailyTruckActivity,
		iot.LabelBreakdownFrequency:            iot.NewTruckBreakdownFrequency,
	},
	"intel": {
		intel.LabelAllMetricsForHosts + "-1-1":        intel.NewAllMetricsForHosts(1, 1*time.Hour),
		intel.LabelAllMetricsForHosts + "-1-48":       intel.NewAllMetricsForHosts(1, 48*time.Hour),
		intel.LabelAllMetricsForCluster + "-1-48":     intel.NewAllMetricsForClusters(1, 48*time.Hour),
		intel.LabelLastPointPrimary:                   intel.NewLastPointPrimary,
		intel.LabelLastPointForHosts + "-30":          intel.NewLastPointForHosts(30),
		intel.LabelTopKHostsForCluster + "-1-10-1":    intel.NewTopKHostsFromCluster(10, 1*time.Hour),
		intel.LabelTopKPrimariesForCluster + "-1-1-1": intel.NewTopKPrimariesFromCluster(1, 1*time.Hour),
		intel.LabelAvgMetricsForHosts + "-10-1-1":     intel.NewHourlyAvgMetricsForHosts(10, 1, 6*24*time.Hour),
		intel.LabelClusterDailyAverage:                intel.NewHourlyAvgMetricsForClusters(250, 1, 24*time.Hour),
		intel.LabelCounterRateHost + "-1-48":          intel.NewCounterRateHost(1, 48*time.Hour),
	},
}

var conf = &config.QueryGeneratorConfig{}

// Parse args:
func init() {
	useCaseMatrix["cpu-only"] = useCaseMatrix["devops"]
	// Change the Usage function to print the use case matrix of choices:
	oldUsage := pflag.Usage
	pflag.Usage = func() {
		oldUsage()

		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "The use case matrix of choices is:\n")
		for uc, queryTypes := range useCaseMatrix {
			for qt := range queryTypes {
				fmt.Fprintf(os.Stderr, "  use case: %s, query type: %s\n", uc, qt)
			}
		}
	}

	conf.AddToFlagSet(pflag.CommandLine)

	pflag.Parse()

	err := internalUtils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&conf.BaseConfig); err != nil {
		panic(fmt.Errorf("unable to decode base config: %s", err))
	}

	if err := viper.Unmarshal(&conf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
}

func main() {
	qg := inputs.NewQueryGenerator(useCaseMatrix)
	err := qg.Generate(conf)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
}
