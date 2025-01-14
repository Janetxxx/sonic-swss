mod message;
mod actor;
use std::error::Error;
use message::saistats::{SAIStat, SAIStats, SAIStatsMessage};
use tokio::{spawn, sync::mpsc::channel};

use actor::{netlink::{NetlinkActor, get_genl_family_group}, ipfix::IpfixActor, otel::OtelActor};

use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    tracing::info!("Starting up");

    let (stats_sender, stats_recipient) = channel::<SAIStatsMessage>(100);

    // Create and initialize the OtelActor
    let otel_actor = OtelActor::new(stats_recipient)?;

    let otel_handle = tokio::spawn(async move {
        tracing::info!("Spawning OtelActor");
        otel_actor.run().await;
    });

    let stats = vec![
        SAIStats {
            observation_time: 2,
            stats: vec![
                SAIStat {
                    label: 1, // SAI_OBJECT_TYPE_PORT
                    type_id: 1, // SAI_OBJECT_TYPE_PORT
                    stat_id: 0x00000001, // SAI_PORT_STAT_IF_IN_UCAST_PKTS
                    counter: 3,
                },
                SAIStat {
                    label: 3, // SAI_OBJECT_TYPE_BUFFER_POOL
                    type_id: 24, // SAI_OBJECT_TYPE_BUFFER_POOL
                    stat_id: 0x00000002, // SAI_BUFFER_POOL_STAT_DROPPED_PACKETS
                    counter: 2,
                },
            ],
        }
    ];

    for stat in stats {
        tracing::info!("Sent stats: {:?}", stat);
        stats_sender.send(stat.into()).await?;
    }

    drop(stats_sender);

    // Allow some time for processing before shutdown
    //tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    if let Err(e) =otel_handle.await {
        tracing::error!("Error during OtelActor shutdown: {:?}", e);
    }

    // Shutdown OpenTelemetry resources gracefully
    tracing::info!("Shutting down OtelActor and OpenTelemetry resources.");

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
