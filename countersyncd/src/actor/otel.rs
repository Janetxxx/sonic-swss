use std::{net::Shutdown, sync::Arc, time::Duration};
use tokio::{sync::mpsc::{Receiver, Sender}, sync::oneshot, time::sleep};
use opentelemetry::{global, metrics::{Gauge, MetricsError}, KeyValue};
use opentelemetry_otlp::{ExportConfig, WithExportConfig};
use opentelemetry_sdk::{runtime, Resource};
use opentelemetry_proto::tonic::{
    common::v1::{KeyValue as ProtoKeyValue, AnyValue, any_value::Value},
    metrics::v1::{Metric, Gauge as ProtoGauge, NumberDataPoint, number_data_point},
};
use crate::message::saistats::{SAIStat, SAIStats, SAIStatsMessage};
use crate::actor::saistat_mapper::generate_metric_info;
use log::{info, error};

#[derive(Debug)]
pub struct OtelActor {
    stats_recipient: Receiver<SAIStatsMessage>,
    meter_provider: opentelemetry_sdk::metrics::SdkMeterProvider,
    shutdown_notifier: Option<oneshot::Sender<()>>,
}

impl OtelActor {
    pub fn new(stats_recipient: Receiver<SAIStatsMessage>, shutdown_notifier: oneshot::Sender<()>) -> Result<OtelActor, MetricsError> {
        // Build OTLP pipeline
        let export_config = ExportConfig {
            endpoint: "http://localhost:4317".to_string(),
            protocol: opentelemetry_otlp::Protocol::Grpc,
            timeout: Duration::from_secs(5),
            ..ExportConfig::default()
        };

        let resource = Resource::new(vec![
            KeyValue::new("service.name", "countersyncd"),
        ]);

        let meter_provider = opentelemetry_otlp::new_pipeline()
            .metrics(runtime::Tokio)
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_export_config(export_config),
            )
            .with_resource(resource)
            .build()?;

        global::set_meter_provider(meter_provider.clone());

        Ok(OtelActor {
            stats_recipient,
            meter_provider,
            shutdown_notifier: Some(shutdown_notifier)
        })
    }

    async fn shutdown(self) -> Result<(), MetricsError> {
        info!("Shutting down OpenTelemetry...");

        //force flush
        let flush_result = tokio::task::spawn_blocking({
            let meter_provider = self.meter_provider.clone();
            move || meter_provider.force_flush()
        })
        .await
        .map_err(|e|{
            error!("Tokio task error: {:?}", e);
            MetricsError::Other("Error during OpenTelemetry force flush".into())
        })?;

        if let Err(e) = flush_result {
            error!("Error during OpenTelemetry flush: {:?}", e);
        } else {
            info!("OpenTelemetry flush successful.");
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        // shutdown
        tokio::task::spawn_blocking({
            let meter_provider = self.meter_provider.clone();
            move || {
                if let Err(e) = meter_provider.shutdown() {
                    error!("Error during OpenTelemetry shutdown: {:?}", e);
                }
            }
        })
        .await
        .expect("Shutdown task panicked");

        //Notify main function that shutdown is complete
        if let Some(notifier) = self.shutdown_notifier {
            let _ = notifier.send(());
        }

        info!("OpenTelemetry shutdown complete.");

        Ok(())  // Returning Result<(), MetricsError>
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


    async fn handle_stats(&self, stats: SAIStatsMessage) {
        let stats = stats.clone();
        for sai_stat in &stats.stats {
            self.convert_to_otlp_metric(sai_stat, stats.observation_time).await;
        }
    }

    async fn convert_to_otlp_metric(&self, sai_stat: &SAIStat, observation_time: u64) {
        if let Ok((metric_name, object_label)) = generate_metric_info(
            sai_stat.label as u64,
            sai_stat.type_id as u64,
            sai_stat.stat_id as u64,
        ) {
            // Create raw OTLP protocol buffer objects
            // Create data point with observation time
            let data_point = NumberDataPoint {
                time_unix_nano: observation_time,
                value: Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(sai_stat.counter as i64)),
                attributes: vec![
                    opentelemetry_proto::tonic::common::v1::KeyValue {
                        key: "object_name".to_string(),
                        value: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                            value: Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                object_label.clone()
                            )),
                        }),
                    }
                ],
                ..Default::default()
            };

            // Create the guage with the data point
            let proto_gauge = ProtoGauge {
                data_points: vec![data_point],
            };

            // Create the metric with the gauge
            let metric = Metric {
                name: metric_name.clone(),
                data: Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(proto_gauge)),
                ..Default::default()
            };

            info!(
                "OTLP Metric:\n{:#?}",
                metric
            );

            info!(
                "Successfully created metric: {} with gauge value: {}",
                metric_name, sai_stat.counter
            );
        } else {
            error!("Failed to generate metric info for stat: {:?}", sai_stat);
        }
    }


}
