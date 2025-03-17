use std::{net::Shutdown, sync::Arc, time::Duration};
use tokio::{sync::mpsc::{Receiver, Sender}, sync::oneshot, time::sleep};
use opentelemetry::{global, metrics::{Gauge, MetricsError}, KeyValue};
use opentelemetry_proto::tonic::{
    common::v1::{KeyValue as ProtoKeyValue, AnyValue, any_value::Value, InstrumentationScope},
    metrics::v1::{Metric, Gauge as ProtoGauge, NumberDataPoint, number_data_point, ResourceMetrics, ScopeMetrics},
    resource::v1::Resource as ProtoResource,
};
use crate::message::saistats::{SAIStat, SAIStats, SAIStatsMessage};
use crate::actor::saistat_mapper::generate_metric_info;
use log::{info, error};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use chrono::{DateTime, TimeZone, Utc};



#[derive(Debug)]
pub struct OtelActor {
    stats_recipient: Receiver<SAIStatsMessage>,
    shutdown_notifier: Option<oneshot::Sender<()>>,
    client: MetricsServiceClient<tonic::transport::Channel>,
}

impl OtelActor {
    pub async fn new(stats_recipient: Receiver<SAIStatsMessage>, shutdown_notifier: oneshot::Sender<()>) -> Result<OtelActor, Box<dyn std::error::Error>> {
        let client = MetricsServiceClient::connect("http://localhost:4317").await?;

        Ok(OtelActor {
            stats_recipient,
            shutdown_notifier: Some(shutdown_notifier),
            client,
        })
    }

    async fn shutdown(self) -> Result<(), MetricsError> {
        info!("Shutting down OpenTelemetry...");

        // Give pending requests time to complete
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Notify main function that shutdown is complete
        if let Some(notifier) = self.shutdown_notifier {
            if let Err(e) = notifier.send(()) {
                error!("Failed to send shutdown notification: {:?}", e);
            }
        }

        info!("OpenTelemetry shutdown complete.");

        Ok(())
    }



    pub async fn run(mut self) {
        while let Some(stats) = self.stats_recipient.recv().await {
            tracing::info!("Received stats: {:?}", stats);
            self.handle_stats(stats).await;
        }

        // Logging to verify shutdown flow
        info!("Metrics processing complete, shutting down...");

        // Ensure metrics are exported before shutdown and handle potential error
        if let Err(e) = self.shutdown().await {
            tracing::error!("Error during OpenTelemetry shutdown: {:?}", e);
        }
    }


    async fn handle_stats(&mut self, stats: SAIStatsMessage) {
        let stats = stats.clone();
        for sai_stat in &stats.stats {
            self.convert_to_otlp_metric(sai_stat, stats.observation_time).await;
        }
    }

    async fn convert_to_otlp_metric(&mut self, sai_stat: &SAIStat, observation_time: u64) {
        if let Ok((metric_name, object_label)) = generate_metric_info(
            sai_stat.label as u64,
            sai_stat.type_id as u64,
            sai_stat.stat_id as u64,
        ) {
            // Create raw OTLP protocol buffer objects
            let data_point = NumberDataPoint {
                time_unix_nano: observation_time,
                value: Some(number_data_point::Value::AsInt(sai_stat.counter as i64)),
                attributes: vec![
                    ProtoKeyValue {
                        key: "object_name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue(object_label.clone())),
                        }),
                    }
                ],
                ..Default::default()
            };

            info!("Created data point: {:?}", data_point);
            info!("Formated data point:\n\
                \tattributes: {:?}\n\
                \ttime_unix_nano: {}\n\
                \tvalue: {:?}",
                data_point.attributes,
                data_point.time_unix_nano,
                data_point.value
            );

            // Create the gauge with the data point
            let proto_gauge = ProtoGauge {
                data_points: vec![data_point],
            };

            info!("Created gauge: {:?}", proto_gauge);

            // Create the metric with the gauge
            let metric = Metric {
                name: metric_name.clone(),
                description: "SAI counter statistic".to_string(),
                metadata: vec![
                    ProtoKeyValue {
                        key: "object_name_metadata".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue(object_label))
                        })
                }],
                data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(proto_gauge)),
                ..Default::default()
            };

            info!("Created metric: {:?}", metric);

            // Create ResourceMetrics for export
            let resource_metrics = ResourceMetrics {
                resource: Some(ProtoResource {
                    attributes: vec![ProtoKeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("countersyncd".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "countersyncd".to_string(),
                        version: "1.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    schema_url: "".to_string(),
                    metrics: vec![metric],
                }],
                schema_url: "".to_string(),
            };

            // Create the export request
            let request = ExportMetricsServiceRequest {
                resource_metrics: vec![resource_metrics],
            };

            // Export using gRPC client directly
            match self.client.export(request).await {
                Ok(_) => {
                    info!(
                        "Successfully exported metric: {} with gauge value: {}",
                        metric_name, sai_stat.counter
                    );
                }
                Err(e) => {
                    error!("Failed to export metric: {:?}", e);
                }
            }
        }
    }


}


#[tokio::test]
async fn test_metrics_export_and_prometheus_endpoint() -> Result<(), Box<dyn std::error::Error>> {
    // Set up channels
    let (shutdown_tx, _shutdown_rx) = oneshot::channel();
    let (stats_sender, stats_recipient) = tokio::sync::mpsc::channel(1);

    // Create and spawn OtelActor
    let otel_actor = OtelActor::new(stats_recipient, shutdown_tx).await?;
    let actor_handle = tokio::spawn(async move {
        otel_actor.run().await;
    });

    // Unix timestamp in nanoseconds
    let utc: DateTime<Utc> = Utc::now();
    tracing::info!("UTC timestamp: {:?}", utc);
    let timestamp = utc.timestamp_nanos_opt()
        .expect("Timestamp conversion failed") as u64;

    // Create test stats using the exact data from main.rs
    let test_stats = vec![
        SAIStats {
            observation_time: timestamp,
            stats: vec![
                SAIStat {
                    label: 1,    // SAI_OBJECT_TYPE_PORT, Ethernet1
                    type_id: 1,  // SAI_OBJECT_TYPE_PORT
                    stat_id: 1,  // 0x00000001, SAI_PORT_STAT_IF_IN_UCAST_PKTS
                    counter: 3
                },
                SAIStat {
                    label: 3,    // SAI_OBJECT_TYPE_BUFFER_POOL, BUFFER_POOL_3
                    type_id: 24, // SAI_OBJECT_TYPE_BUFFER_POOL
                    stat_id: 2,  // 0x00000002, SAI_BUFFER_POOL_STAT_DROPPED_PACKETS
                    counter: 2
                },
            ],
        }
    ];

    // Send stats and verify metrics
    for stat in test_stats {
        stats_sender.send(stat.into()).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Wait for metrics to be exported and collected
    sleep(Duration::from_secs(5)).await;

    // Create HTTP client and fetch metrics
    let client = reqwest::Client::new();

    let prometheus_response = client.get("http://localhost:8086/metrics")
        .send()
        .await?
        .text()
        .await?;

    //tracing::debug!("Received metrics:\n{}", prometheus_response);

    // Verify the exact metrics we expect
    assert!(prometheus_response.contains("port_if_in_ucast_pkts"));
    assert!(prometheus_response.contains("buffer_pool_dropped_packets"));
    assert!(prometheus_response.contains("object_name=\"Ethernet1\""));
    assert!(prometheus_response.contains("object_name=\"BUFFER_POOL_3\""));
    assert!(prometheus_response.contains("} 3")); // Port stat value
    assert!(prometheus_response.contains("} 2")); // Buffer pool stat value

    // Verify metric types
    assert!(prometheus_response.contains("# TYPE port_if_in_ucast_pkts gauge"));
    assert!(prometheus_response.contains("# TYPE buffer_pool_dropped_packets gauge"));

    // Cleanup
    drop(stats_sender);
    actor_handle.await?;

    Ok(())

}