pub mod message;
pub mod actor;
use std::error::Error;
use message::saistats::{SAIStat, SAIStats, SAIStatsMessage};
use tokio::{spawn, sync::mpsc::channel};
use tokio::{sync::oneshot, task};
use actor::{netlink::{NetlinkActor, get_genl_family_group}, ipfix::IpfixActor, otel::OtelActor};

use tracing_subscriber::{EnvFilter, fmt, prelude::*};
use chrono::{DateTime, TimeZone, Utc, Timelike};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    tracing::info!("Starting up");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (stats_sender, stats_recipient) = channel::<SAIStatsMessage>(1);

    // Create and initialize the OtelActor
    let otel_actor = OtelActor::new(stats_recipient, shutdown_tx).await?;

    let otel_handle = tokio::spawn(async move {
        tracing::info!("Spawning OtelActor");
        otel_actor.run().await;
    });

    // Unix timestamp in nanoseconds
    // let utc: DateTime<Utc> = Utc::now();
    // tracing::info!("UTC timestamp: {:?}", utc);
    // let timestamp = utc.timestamp_nanos_opt()
    //     .expect("Timestamp conversion failed") as u64;

    // Custom Unix timestamp in nanoseconds
    let utc = Utc.with_ymd_and_hms(2025, 3, 01, 15, 30, 0)
        .single()
        .expect("Invalid date time")
        .with_nanosecond(0)
        .expect("Invalid nanosecond");
    tracing::info!("UTC timestamp: {:?}", utc);
    let timestamp = utc.timestamp_nanos_opt()
        .expect("Timestamp1 conversion failed") as u64;

    //Create test metrics for both InfluxDB instances
    let port_stats = SAIStats {
        observation_time: timestamp,
        stats: vec![
            SAIStat {
                label: 1,     // Ethernet1
                type_id: 1,   // SAI_OBJECT_TYPE_PORT
                stat_id: 1,   // SAI_PORT_STAT_IF_IN_UCAST_PKTS
                counter: 100
            },
            SAIStat {
                label: 2,     // Ethernet2
                type_id: 1,   // SAI_OBJECT_TYPE_PORT
                stat_id: 1,   // SAI_PORT_STAT_IF_IN_UCAST_PKTS
                counter: 200
            },
        ],
    };

    let buffer_stats = SAIStats {
        observation_time: timestamp,
        stats: vec![
            SAIStat {
                label: 3,     // BUFFER_POOL_3
                type_id: 24,  // SAI_OBJECT_TYPE_BUFFER_POOL
                stat_id: 2,   // SAI_BUFFER_POOL_STAT_DROPPED_PACKETS
                counter: 50
            },
            SAIStat {
                label: 4,     // BUFFER_POOL_4
                type_id: 24,  // SAI_OBJECT_TYPE_BUFFER_POOL
                stat_id: 2,   // SAI_BUFFER_POOL_STAT_DROPPED_PACKETS
                counter: 75
            },
        ],
    };

    // Send port metrics
    tracing::info!("Sending port metrics to InfluxDB A");
    stats_sender.send(port_stats.into()).await?;

    // // Wait to ensure processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Send buffer pool metrics
    tracing::info!("Sending buffer pool metrics to InfluxDB B");
    stats_sender.send(buffer_stats.into()).await?;

    // Allow time for processing and export
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Query both InfluxDB instances to verify routing
    tracing::info!("Verifying routing by querying both InfluxDB instances");

    // Create HTTP client
    //let client = reqwest::Client::new();

    // Query InfluxDB A for port metrics
    // let response_a = client.post("http://localhost:8086/api/v2/query")
    // .header("Authorization", "Token mytoken123456789")
    // .header("Content-Type", "application/vnd.flux")
    // .body("from(bucket:\"ports\") |> range(start: -1h) |> filter(fn: (r) => r._measurement =~ /port.*/)")
    // .send()
    // .await?
    // .text()
    // .await?;

    // Query InfluxDB B for buffer metrics
    // let response_b = client.post("http://localhost:8086/api/v2/query")
    // .header("Authorization", "Token mytoken123456789")
    // .header("Content-Type", "application/vnd.flux")
    // .body("from(bucket:\"buffers\") |> range(start: -1h) |> filter(fn: (r) => r._measurement =~ /buffer_pool.*/)")
    // .send()
    // .await?
    // .text()
    // .await?;

    // Check if responses contain data
    // if response_a.contains("port_if_in_ucast_pkts") || !response_a.contains("\"results\":[{}]") {
    // tracing::info!("✅ Port metrics found in InfluxDB A");
    // } else {
    // tracing::error!("❌ Port metrics NOT found in InfluxDB A");
    // }

    // More accurate check for actual data presence
    // if response_b.contains("buffer_pool_dropped_packets") ||
    // (response_b.contains("_measurement") && !response_b.contains("\"results\":[{}]")) {
    //     tracing::info!("✅ Buffer metrics found in InfluxDB B");
    //     tracing::debug!("Response data: {}", &response_b);
    // } else {
    //     tracing::error!("❌ Buffer metrics NOT found in InfluxDB B");
    //     tracing::debug!("Full response: {}", response_b);
    // }

    // Clean shutdown
    drop(stats_sender);

    if let Err(e) = shutdown_rx.await {
        tracing::error!("Failed to receive shutdown signal: {:?}", e);
    }

    if let Err(e) = otel_handle.await {
        tracing::error!("Error during OtelActor shutdown: {:?}", e);
    }

    tracing::info!("Test completed");

    Ok(())

}


// mod message;
// mod actor;

// use tokio::{spawn, sync::mpsc::channel};

// use actor::{netlink::{NetlinkActor, get_genl_family_group}, ipfix::IpfixActor};

// #[tokio::main]
// async fn main() {
//     let (_command_sender, command_receiver) = channel(1);
//     let (socket_sender, socket_receiver) = channel(1);
//     let (_ipfix_template_sender, ipfix_template_receiver) = channel(1);
//     let (saistats_sender, _saistats_receiver) = channel(1);

//     let (family, group) = get_genl_family_group();

//     let mut netlink = NetlinkActor::new(family.as_str(), group.as_str(), command_receiver);
//     netlink.add_recipient(socket_sender);
//     let mut ipfix = IpfixActor::new(ipfix_template_receiver, socket_receiver);
//     ipfix.add_recipient(saistats_sender);

//     let netlink_handle = spawn(NetlinkActor::run(netlink));
//     let ipfix_handle = spawn(IpfixActor::run(ipfix));

//     netlink_handle.await.unwrap();
//     ipfix_handle.await.unwrap();
// }
