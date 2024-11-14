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

// GetSystemState - Gets system information about a Google Cloud SQL instance
func GetSystemState(ctx context.Context, server *state.Server, logger *util.Logger) (system state.SystemState) {
	config := server.Config
	system.Info.Type = state.GoogleCloudSQLSystem

	if config.GcpCredentialsFile == "" && config.GcpProjectID == "" {
		server.SelfTest.MarkCollectionAspectError(state.CollectionAspectSystemStats, "missing GCP credentials or project ID")
		logger.PrintError("CloudSQL/System: Missing GCP credentials or project ID")
		return
	}

	var opts []option.ClientOption
	if config.GcpCredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(config.GcpCredentialsFile))
	}

	client, err := newMetricClientFunc(ctx, opts...)
	if err != nil {
		server.SelfTest.MarkCollectionAspectError(state.CollectionAspectSystemStats, "error creating monitoring client: %v", err)
		logger.PrintError("CloudSQL/System: Failed to create monitoring client: %v", err)
		return
	}
	defer client.Close()

	instanceID := config.GcpCloudSQLInstanceID
	filter := fmt.Sprintf(
		`resource.type = "cloudsql_database" AND resource.labels.database_id = "%s:%s"`,
		config.GcpProjectID,
		instanceID,
	)

	// Create base request template
	baseReq := &monitoringpb.ListTimeSeriesRequest{
		Name:   fmt.Sprintf("projects/%s", config.GcpProjectID),
		Filter: filter,
		Interval: &monitoringpb.TimeInterval{
			EndTime:   &timestamppb.Timestamp{Seconds: time.Now().Unix()},
			StartTime: &timestamppb.Timestamp{Seconds: time.Now().Add(-5 * time.Minute).Unix()},
		},
	}

	// Memory stats
	system.Memory.TotalBytes = uint64(getIntMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/memory/quota"))

	usedMemory := uint64(getIntMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/memory/usage"))

	system.Memory.FreeBytes = system.Memory.TotalBytes - usedMemory

	system.Memory.SwapUsedBytes = uint64(getIntMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/swap/bytes_used"))

	// CPU Stats
	system.CPUStats = make(state.CPUStatisticMap)
	cpuUtil := getPercentMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/cpu/utilization")
	system.CPUStats["all"] = state.CPUStatistic{
		DiffedOnInput: true,
		DiffedValues: &state.DiffedSystemCPUStats{
			UserPercent: cpuUtil,
		},
	}

	system.Disks = make(state.DiskMap)
	system.Disks["default"] = state.Disk{
		DiskType: "N/A",
	}

	// Disk stats
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

	// Disk partitions
	system.DiskPartitions = make(state.DiskPartitionMap)
	usedBytes := uint64(getIntMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/disk/bytes_used"))
	totalBytes := uint64(getIntMetric(ctx, client, cloneRequest(baseReq), "cloudsql.googleapis.com/database/disk/quota"))

	system.DiskPartitions["/"] = state.DiskPartition{
		DiskName:      "cloudsql",
		PartitionName: "cloudsql",
		UsedBytes:     usedBytes,
		TotalBytes:    totalBytes,
	}

	// Network stats
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

	server.SelfTest.MarkCollectionAspectOk(state.CollectionAspectSystemStats)

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
		value := ts.Points[0].Value.GetDoubleValue() * 100
		return value
	}

	log.Printf("No points found in percentage metrics for type %s", metricType)
	return 0
}

func getIntMetric(ctx context.Context, client *monitoring.MetricClient, req *monitoringpb.ListTimeSeriesRequest, metricType string) int64 {
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
		value := ts.Points[0].Value.GetInt64Value()
		return value
	}

	log.Printf("No points found in integer metrics for type %s", metricType)
	return 0
}
