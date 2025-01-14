use std::sync::Arc;
use tokio::{sync::mpsc::{Receiver, Sender}, time::timeout};
use opentelemetry::{global, metrics::{Counter, MetricsError}, KeyValue};
use opentelemetry_otlp::{ExportConfig, WithExportConfig};
use opentelemetry_sdk::{runtime, Resource};
use crate::message::saistats::{SAIStat, SAIStats, SAIStatsMessage};
use crate::actor::saistat_mapper::generate_counter_name;
use log::{info, error};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug)]
pub struct OtelActor {
    stats_recipient: Receiver<SAIStatsMessage>,
    meter_provider: opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl OtelActor {
    pub fn new(stats_recipient: Receiver<SAIStatsMessage>) -> Result<OtelActor, MetricsError> {
        // Build OTLP pipeline
        let export_config = ExportConfig {
            endpoint: "http://localhost:4317".to_string(),
            protocol: opentelemetry_otlp::Protocol::Grpc,
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
        })
    }

    async fn shutdown(&self) -> Result<(), MetricsError> {
        info!("Shutting down OpenTelemetry...");


        let flush_result = tokio::task::spawn_blocking({
            let meter_provider = self.meter_provider.clone();
            move || meter_provider.force_flush()
        })
        .await
        .map_err(|e|{
            error!("Tokio task error: {:?}", e);
            MetricsError::Other("Tokio task error".into())
        })?;

        if let Err(e) = flush_result {
            error!("Error during OpenTelemetry flush: {:?}", e);
        } else {
            info!("OpenTelemetry flush successful.");
        }

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
        let meter = global::meter_with_version(
            env!("CARGO_PKG_NAME"),
            Some(env!("CARGO_PKG_VERSION")),
            Some(opentelemetry_semantic_conventions::SCHEMA_URL),
            None,
        );

        // Generate a more descriptive counter name
        if let Ok(counter_name) = generate_counter_name(sai_stat.label as u64, sai_stat.type_id as u64, sai_stat.stat_id as u64) {
            // Create or get the counter from the meter
            let counter: Counter<u64> = meter.u64_counter(counter_name.clone()).init();

            // Add value to the counter
            counter.add(sai_stat.counter, &[KeyValue::new("observation_time", observation_time.to_string())]);

            info!("Successfully created metric: {} with value: {}", counter_name.clone(), sai_stat.counter);
        } else {
            error!("Failed to generate counter name for stat: {:?}", sai_stat);
        }
    }


}
