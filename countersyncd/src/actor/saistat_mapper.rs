use std::collections::HashMap;
use lazy_static::lazy_static;

lazy_static! {
    // This module provides mappings from SAIStats message numbers to their corresponding string names.
    static ref COUNTER_TYPE_MAP: HashMap<u64, &'static str> = {
        let mut map = HashMap::new();
        map.insert(1, "SAI_OBJECT_TYPE_PORT");
        map.insert(24, "SAI_OBJECT_TYPE_BUFFER_POOL");
        map.insert(21, "SAI_OBJECT_TYPE_QUEUE");
        map.insert(26, "SAI_OBJECT_TYPE_INGRESS_PRIORITY_GROUP");
        map
    };

    static ref PORT_STATS_MAP: HashMap<u64, &'static str> = {
        let mut map = HashMap::new();
        map.insert(0x00000000, "SAI_PORT_STAT_IF_IN_OCTETS");
        map.insert(0x00000001, "SAI_PORT_STAT_IF_IN_UCAST_PKTS");
        map.insert(0x00000003, "SAI_PORT_STAT_IF_IN_DISCARDS");
        map.insert(0x00000004, "SAI_PORT_STAT_IF_IN_ERRORS");
        map.insert(0x00000009, "SAI_PORT_STAT_IF_OUT_OCTETS");
        map.insert(0x0000000A, "SAI_PORT_STAT_IF_OUT_UCAST_PKTS");
        map.insert(0x0000000C, "SAI_PORT_STAT_IF_OUT_DISCARDS");
        map.insert(0x0000000D, "SAI_PORT_STAT_IF_OUT_ERRORS");
        map.insert(0x0000005B, "SAI_PORT_STAT_IN_CURR_OCCUPANCY_BYTES");
        map.insert(0x0000005F, "SAI_PORT_STAT_OUT_CURR_OCCUPANCY_BYTES");
        map
    };

    static ref QUEUE_STATS_MAP: HashMap<u64, &'static str> = {
        let mut map = HashMap::new();
        map.insert(0x00000000, "SAI_QUEUE_STAT_PACKETS");
        map.insert(0x00000001, "SAI_QUEUE_STAT_BYTES");
        map.insert(0x00000002, "SAI_QUEUE_STAT_DROPPED_PACKETS");
        map.insert(0x00000018, "SAI_QUEUE_STAT_CURR_OCCUPANCY_BYTES");
        map.insert(0x00000019, "SAI_QUEUE_STAT_WATERMARK_BYTES");
        map.insert(0x00000022, "SAI_QUEUE_STAT_WRED_ECN_MARKED_PACKETS");
        map
    };

    static ref PG_STATS_MAP: HashMap<u64, &'static str> = {
        let mut map = HashMap::new();
        map.insert(0x00000000, "SAI_INGRESS_PRIORITY_GROUP_STAT_PACKETS");
        map.insert(0x00000001, "SAI_INGRESS_PRIORITY_GROUP_STAT_BYTES");
        map.insert(0x00000002, "SAI_INGRESS_PRIORITY_GROUP_STAT_CURR_OCCUPANCY_BYTES");
        map.insert(0x00000003, "SAI_INGRESS_PRIORITY_GROUP_STAT_WATERMARK_BYTES");
        map.insert(0x00000006, "SAI_INGRESS_PRIORITY_GROUP_STAT_XOFF_ROOM_CURR_OCCUPANCY_BYTES");
        map.insert(0x00000007, "SAI_INGRESS_PRIORITY_GROUP_STAT_XOFF_ROOM_WATERMARK_BYTES");
        map.insert(0x00000008, "SAI_INGRESS_PRIORITY_GROUP_STAT_DROPPED_PACKETS");
        map
    };

    static ref BUFFER_POOL_STATS_MAP: HashMap<u64, &'static str> = {
        let mut map = HashMap::new();
        map.insert(0x00000000, "SAI_BUFFER_POOL_STAT_CURR_OCCUPANCY_BYTES");
        map.insert(0x00000001, "SAI_BUFFER_POOL_STAT_WATERMARK_BYTES");
        map.insert(0x00000002, "SAI_BUFFER_POOL_STAT_DROPPED_PACKETS");
        map.insert(0x00000014, "SAI_BUFFER_POOL_STAT_XOFF_ROOM_WATERMARK_BYTES");
        map
    };
}

fn get_counter_type_name(counter_type: u64) -> Result<&'static str, String> {
    COUNTER_TYPE_MAP.get(&counter_type).copied().ok_or_else(|| format!("Counter type {} not found", counter_type))
}

fn get_port_stat_name(stat: u64) -> Result<&'static str, String> {
    PORT_STATS_MAP.get(&stat).copied().ok_or_else(|| format!("Port stat {} not found", stat))
}

fn get_queue_stat_name(stat: u64) -> Result<&'static str, String> {
    QUEUE_STATS_MAP.get(&stat).copied().ok_or_else(|| format!("Queue stat {} not found", stat))
}

fn get_pg_stat_name(stat: u64) -> Result<&'static str, String> {
    PG_STATS_MAP.get(&stat).copied().ok_or_else(|| format!("PG stat {} not found", stat))
}

fn get_buffer_pool_stat_name(stat: u64) -> Result<&'static str, String> {
    BUFFER_POOL_STATS_MAP.get(&stat).copied().ok_or_else(|| format!("Buffer pool stat {} not found", stat))
}

/// Generates a counter name based on the provided label, type ID, and stat ID.
pub(crate) fn generate_counter_name(label: u64, type_id: u64, stat_id: u64) -> Result<String, String> {
    let object_type = get_counter_type_name(type_id)?;
    let label_name = match type_id {
        1 => format!("Ethernet{}", label),
        21 => format!("QUEUE_{}", label),
        24 => format!("BUFFER_POOL_{}", label),
        26 => format!("PG_{}", label),
        _ => return Err(format!("Unknown type_id {}", type_id)),
    };
    let stat_name = match type_id {
        1 => get_port_stat_name(stat_id),
        21 => get_queue_stat_name(stat_id),
        24 => get_buffer_pool_stat_name(stat_id),
        26 => get_pg_stat_name(stat_id),
        _ => return Err(format!("Unknown type_id {}", type_id)),
    }?;

    Ok(format!("{}_{}_{}", object_type, label_name, stat_name))
}