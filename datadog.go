// Copyright 2022 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"context"
	"fmt"
	"net/http"

	datadog "github.com/DataDog/datadog-api-client-go/api/v1/datadog"
)

// Write new stats to DataDog
func datadogUploadStats(hostname string, addedStats map[string][]AppLBStat) (err error) {

	// Generate the list of aggregated stats
	bucketSecs, aggregatedStats := statsAggregate(addedStats)
	if bucketSecs == 0 || len(aggregatedStats) == 0 {
		return
	}

	// Create the metrics
	var series datadog.Series
	seriesArray := []datadog.Series{}

	series = datadog.Series{Metric: "notehub." + hostname + ".disk.reads", Type: datadog.PtrString("gauge")}
	for _, stat := range aggregatedStats {
		point := []*float64{
			datadog.PtrFloat64(float64(stat.Time)),
			datadog.PtrFloat64(float64(stat.DiskReads)),
		}
		series.Points = append(series.Points, point)
	}
	seriesArray = append(seriesArray, series)

	series = datadog.Series{Metric: "notehub." + hostname + ".disk.writes", Type: datadog.PtrString("gauge")}
	for _, stat := range aggregatedStats {
		point := []*float64{
			datadog.PtrFloat64(float64(stat.Time)),
			datadog.PtrFloat64(float64(stat.DiskWrites)),
		}
		series.Points = append(series.Points, point)
	}
	seriesArray = append(seriesArray, series)

	series = datadog.Series{Metric: "notehub." + hostname + ".net.received", Type: datadog.PtrString("gauge")}
	for _, stat := range aggregatedStats {
		point := []*float64{
			datadog.PtrFloat64(float64(stat.Time)),
			datadog.PtrFloat64(float64(stat.NetReceived)),
		}
		series.Points = append(series.Points, point)
	}
	seriesArray = append(seriesArray, series)

	series = datadog.Series{Metric: "notehub." + hostname + ".net.sent", Type: datadog.PtrString("gauge")}
	for _, stat := range aggregatedStats {
		point := []*float64{
			datadog.PtrFloat64(float64(stat.Time)),
			datadog.PtrFloat64(float64(stat.NetSent)),
		}
		series.Points = append(series.Points, point)
	}
	seriesArray = append(seriesArray, series)

	series = datadog.Series{Metric: "notehub." + hostname + ".handlers", Type: datadog.PtrString("gauge")}
	for _, stat := range aggregatedStats {
		point := []*float64{
			datadog.PtrFloat64(float64(stat.Time)),
			datadog.PtrFloat64(float64(stat.Handlers)),
		}
		series.Points = append(series.Points, point)
	}
	seriesArray = append(seriesArray, series)

	series = datadog.Series{Metric: "notehub." + hostname + ".events.received", Type: datadog.PtrString("gauge")}
	for _, stat := range aggregatedStats {
		point := []*float64{
			datadog.PtrFloat64(float64(stat.Time)),
			datadog.PtrFloat64(float64(stat.EventsReceived)),
		}
		series.Points = append(series.Points, point)
	}
	seriesArray = append(seriesArray, series)

	series = datadog.Series{Metric: "notehub." + hostname + ".events.routed", Type: datadog.PtrString("gauge")}
	for _, stat := range aggregatedStats {
		point := []*float64{
			datadog.PtrFloat64(float64(stat.Time)),
			datadog.PtrFloat64(float64(stat.EventsRouted)),
		}
		series.Points = append(series.Points, point)
	}
	seriesArray = append(seriesArray, series)

	series = datadog.Series{Metric: "notehub." + hostname + ".database.reads", Type: datadog.PtrString("gauge")}
	for _, stat := range aggregatedStats {
		point := []*float64{
			datadog.PtrFloat64(float64(stat.Time)),
			datadog.PtrFloat64(float64(stat.DatabaseReads)),
		}
		series.Points = append(series.Points, point)
	}
	seriesArray = append(seriesArray, series)

	series = datadog.Series{Metric: "notehub." + hostname + ".database.writes", Type: datadog.PtrString("gauge")}
	for _, stat := range aggregatedStats {
		point := []*float64{
			datadog.PtrFloat64(float64(stat.Time)),
			datadog.PtrFloat64(float64(stat.DatabaseWrites)),
		}
		series.Points = append(series.Points, point)
	}
	seriesArray = append(seriesArray, series)

	series = datadog.Series{Metric: "notehub." + hostname + ".api.calls", Type: datadog.PtrString("gauge")}
	for _, stat := range aggregatedStats {
		point := []*float64{
			datadog.PtrFloat64(float64(stat.Time)),
			datadog.PtrFloat64(float64(stat.APICalls)),
		}
		series.Points = append(series.Points, point)
	}
	seriesArray = append(seriesArray, series)

	// Submit the metrics
	ctx := context.Background()
	ctx = context.WithValue(ctx, datadog.ContextServerVariables, map[string]string{"site": Config.DatadogSite})
	keys := make(map[string]datadog.APIKey)
	keys["apiKeyAuth"] = datadog.APIKey{Key: Config.DatadogAPIKey}
	keys["appKeyAuth"] = datadog.APIKey{Key: Config.DatadogAppKey}
	ctx = context.WithValue(ctx, datadog.ContextAPIKeys, keys)
	configuration := datadog.NewConfiguration()
	apiClient := datadog.NewAPIClient(configuration)
	body := datadog.MetricsPayload{Series: seriesArray}
	var r *http.Response
	_, r, err = apiClient.MetricsApi.SubmitMetrics(ctx, body, *datadog.NewSubmitMetricsOptionalParameters())
	if err != nil {
		fmt.Printf("datadog: error submitting metrics: %s\n", err)
		fmt.Printf("%v\n", r)
	}

	// Done
	return

}
