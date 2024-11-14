package google_cloudsql

import (
	"context"
	"fmt"
	"log"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/pganalyze/collector/state"
	"github.com/pganalyze/collector/util"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var newMetricClientFunc = monitoring.NewMetricClient

func GetSystemState(ctx context.Context, server *state.Server, logger *util.Logger) (system state.SystemState) {
	logger.PrintInfo("CloudSQL/System: Started GetSystemState")

	config := server.Config
	system.Info.Type = state.GoogleCloudSQLSystem
	logger.PrintInfo("CloudSQL/System: Set system type to GoogleCloudSQLSystem")

	if config.GcpCredentialsFile == "" && config.GcpProjectID == "" {
		server.SelfTest.MarkCollectionAspectError(state.CollectionAspectSystemStats, "missing GCP credentials or project ID")
		logger.PrintError("CloudSQL/System: Missing GCP credentials or project ID")
		return
	}
	logger.PrintInfo("CloudSQL/System: Validated GCP credentials configuration")

	var opts []option.ClientOption
	if config.GcpCredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(config.GcpCredentialsFile))
		logger.PrintInfo("CloudSQL/System: Using GCP credentials file: %s", config.GcpCredentialsFile)
	}

	client, err := newMetricClientFunc(ctx, opts...)
	if err != nil {
		server.SelfTest.MarkCollectionAspectError(state.CollectionAspectSystemStats, "error creating monitoring client: %v", err)
		logger.PrintError("CloudSQL/System: Failed to create monitoring client: %v", err)
		return
	}
	logger.PrintInfo("CloudSQL/System: Successfully created monitoring client")
	defer client.Close()

	instanceID := config.GcpCloudSQLInstanceID
	filter := fmt.Sprintf(
		`resource.type = "cloudsql_database" AND resource.labels.database_id = "%s:%s"`,
		config.GcpProjectID,
		instanceID,
	)
	logger.PrintInfo("CloudSQL/System: Created filter for instance %s: %s", instanceID, filter)

	baseReq := &monitoringpb.ListTimeSeriesRequest{
		Name:   fmt.Sprintf("projects/%s", config.GcpProjectID),
		Filter: filter,
		Interval: &monitoringpb.TimeInterval{
			EndTime:   &timestamppb.Timestamp{Seconds: time.Now().Unix()},
			StartTime: &timestamppb.Timestamp{Seconds: time.Now().Add(-5 * time.Minute).Unix()},
		},
	}
	logger.PrintInfo("CloudSQL/System: Created base request template")

	// Memory stats
	logger.PrintInfo("CloudSQL/System: Starting memory metrics collection")
	system.Memory.TotalBytes = uint64(getIntMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/memory/quota"))
	logger.PrintInfo("CloudSQL/System: Total memory bytes: %d", system.Memory.TotalBytes)

	usedMemory := uint64(getIntMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/memory/usage"))
	logger.PrintInfo("CloudSQL/System: Used memory bytes: %d", usedMemory)

	system.Memory.FreeBytes = system.Memory.TotalBytes - usedMemory
	logger.PrintInfo("CloudSQL/System: Free memory bytes: %d", system.Memory.FreeBytes)

	system.Memory.SwapUsedBytes = uint64(getIntMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/swap/bytes_used"))
	logger.PrintInfo("CloudSQL/System: Swap used bytes: %d", system.Memory.SwapUsedBytes)

	// CPU Stats
	logger.PrintInfo("CloudSQL/System: Starting CPU metrics collection")
	system.CPUStats = make(state.CPUStatisticMap)
	cpuUtil := getPercentMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/cpu/utilization")
	system.CPUStats["all"] = state.CPUStatistic{
		DiffedOnInput: true,
		DiffedValues: &state.DiffedSystemCPUStats{
			UserPercent: cpuUtil,
		},
	}
	logger.PrintInfo("CloudSQL/System: CPU utilization: %.2f%%", cpuUtil)

	system.Disks = make(state.DiskMap)
	system.Disks["default"] = state.Disk{
		DiskType: "N/A",
	}

	// Disk stats
	logger.PrintInfo("CloudSQL/System: Starting disk metrics collection")
	system.DiskStats = make(state.DiskStatsMap)
	readOps := float64(getIntMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/disk/read_ops_count"))
	writeOps := float64(getIntMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/disk/write_ops_count"))
	diskUtil := float64(getPercentMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/disk/utilization"))

	system.DiskStats["default"] = state.DiskStats{
		DiffedOnInput: true,
		DiffedValues: &state.DiffedDiskStats{
			ReadOperationsPerSecond:  readOps,
			WriteOperationsPerSecond: writeOps,
			UtilizationPercent:       diskUtil,
		},
	}
	logger.PrintInfo("CloudSQL/System: Disk metrics - Read ops: %.2f, Write ops: %.2f, Utilization: %.2f%%",
		readOps, writeOps, diskUtil)

	// Disk partitions
	logger.PrintInfo("CloudSQL/System: Starting disk partition metrics collection")
	system.DiskPartitions = make(state.DiskPartitionMap)
	usedBytes := uint64(getIntMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/disk/bytes_used"))
	totalBytes := uint64(getIntMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/disk/quota"))

	system.DiskPartitions["/"] = state.DiskPartition{
		DiskName:      "cloudsql",
		PartitionName: "cloudsql",
		UsedBytes:     usedBytes,
		TotalBytes:    totalBytes,
	}
	logger.PrintInfo("CloudSQL/System: Disk partition metrics - Used bytes: %d, Total bytes: %d",
		usedBytes, totalBytes)

	// Network stats
	logger.PrintInfo("CloudSQL/System: Starting network metrics collection")
	system.NetworkStats = make(state.NetworkStatsMap)
	receivedBytes := uint64(getIntMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/network/received_bytes_count"))
	sentBytes := uint64(getIntMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/network/sent_bytes_count"))

	system.NetworkStats["default"] = state.NetworkStats{
		DiffedOnInput: true,
		DiffedValues: &state.DiffedNetworkStats{
			ReceiveThroughputBytesPerSecond:  receivedBytes,
			TransmitThroughputBytesPerSecond: sentBytes,
		},
	}
	logger.PrintInfo("CloudSQL/System: Network metrics - Received bytes/sec: %d, Sent bytes/sec: %d",
		receivedBytes, sentBytes)

	server.SelfTest.MarkCollectionAspectOk(state.CollectionAspectSystemStats)
	logger.PrintInfo("CloudSQL/System: Successfully completed GetSystemState")

	return
}

func cloneRequest(req *monitoringpb.ListTimeSeriesRequest) *monitoringpb.ListTimeSeriesRequest {
	return &monitoringpb.ListTimeSeriesRequest{
		Name:     req.Name,
		Filter:   req.Filter,
		Interval: req.Interval,
	}
}

func getPercentMetric(ctx context.Context, client *monitoring.MetricClient, req *monitoringpb.ListTimeSeriesRequest, metricType string) float64 {
	log.Printf("Getting percentage metric for type %s", metricType)
	req.Filter = fmt.Sprintf("%s AND metric.type = %q", req.Filter, metricType)

	it := client.ListTimeSeries(ctx, req)
	ts, err := it.Next()
	if err == iterator.Done {
		log.Printf("No percentage metrics found for type %s", metricType)
		return 0
	}
	if err != nil {
		log.Printf("Error fetching percentage metrics for type %s: %v", metricType, err)
		return 0
	}

	if len(ts.Points) > 0 {
		// Log all points for debugging
		for i, point := range ts.Points {
			log.Printf("Point %d: %v", i, point.Value.GetDoubleValue())
		}
		value := ts.Points[0].Value.GetDoubleValue() * 100
		return value
	}

	log.Printf("No points found in percentage metrics for type %s", metricType)
	return 0
}

func getIntMetric(ctx context.Context, client *monitoring.MetricClient, req *monitoringpb.ListTimeSeriesRequest, metricType string) int64 {
	log.Printf("Getting integer metric for type %s", metricType)
	req.Filter = fmt.Sprintf("%s AND metric.type = %q", req.Filter, metricType)

	it := client.ListTimeSeries(ctx, req)
	ts, err := it.Next()
	if err == iterator.Done {
		log.Printf("No integer metrics found for type %s", metricType)
		return 0
	}
	if err != nil {
		log.Printf("Error fetching integer metrics for type %s: %v", metricType, err)
		return 0
	}

	if len(ts.Points) > 0 {
		// Log all points for debugging
		for i, point := range ts.Points {
			log.Printf("Point %d: %v", i, point.Value.GetInt64Value())
		}
		value := ts.Points[0].Value.GetInt64Value()
		return value
	}

	log.Printf("No points found in integer metrics for type %s", metricType)
	return 0
}
