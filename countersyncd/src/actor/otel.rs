use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use opentelemetry::{global, metrics::Counter, KeyValue};
use opentelemetry_otlp::{ExportConfig, WithExportConfig};
use opentelemetry_sdk::{runtime, Resource};
use crate::message::saistats::{SAIStat, SAIStats, SAIStatsMessage};
use crate::actor::saistat_mapper::generate_counter_name;
use log::{info, error};

pub struct OtelActor {
    stats_recipient: Receiver<SAIStatsMessage>,
    provider: opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl OtelActor {
    pub fn new(stats_recipient: Receiver<SAIStatsMessage>) -> Self {
        // Build OTLP pipeline
        let export_config = ExportConfig {
            endpoint: "http://localhost:4317".to_string(),
            protocol: opentelemetry_otlp::Protocol::Grpc,
            ..ExportConfig::default()
        };

        let resource = Resource::new(vec![
            KeyValue::new("service.name", "countersyncd"),
        ]);

        let provider = opentelemetry_otlp::new_pipeline()
            .metrics(runtime::Tokio)
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_export_config(export_config),
            )
            .with_resource(resource)
            .build()
            .expect("Failed to build OTLP pipeline");

        global::set_meter_provider(provider.clone());

        OtelActor {
            stats_recipient,
            provider,
        }
    }

    pub async fn run(mut self) {
        while let Some(stats) = self.stats_recipient.recv().await {
            self.handle_stats(stats).await;
        }

        self.provider.shutdown().unwrap_or_else(|e| {
            eprintln!("OTLP metrics shutdown error: {:?}", e);
        });
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
            // Ensure counter name conforms to the metrics naming convention
            let counter_name = counter_name.replace('.', "_");

            // Create or get the counter from the meter
            let counter: Counter<u64> = meter.u64_counter(counter_name.clone()).init();

            // Add value to the counter
            counter.add(sai_stat.counter, &[KeyValue::new("observation_time", observation_time.to_string())]);

            info!("Successfully updated metric: {} with value: {}", counter_name.clone(), sai_stat.counter);
        } else {
            error!("Failed to generate counter name for stat: {:?}", sai_stat);
        }
    }

    fn shutdown(&self) {
        self.provider.shutdown().unwrap();
    }


}
