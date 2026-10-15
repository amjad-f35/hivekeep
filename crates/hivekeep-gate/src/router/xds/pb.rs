//! Small constructors for the protobuf wrappers the builders use everywhere.

use std::time::Duration;

use envoy_types::pb::envoy::config::core::v3::{
    address, config_source, data_source, socket_address, transport_socket, Address,
    AggregatedConfigSource, ApiVersion, ConfigSource, DataSource, SocketAddress, TransportSocket,
};
use envoy_types::pb::google::protobuf::{Any, BoolValue, Duration as PbDuration, UInt32Value};
use prost::{Message, Name};

/// Packs `msg` into an `Any` typed `type.googleapis.com/<full name>`.
pub fn any<T: Message + Name>(msg: T) -> Any {
    envoy_types::util::pack_any(msg)
}

/// Unpacks an `Any`, checking its type URL.
pub fn unpack<T: Message + Name + Default>(any: &Any) -> Result<T, String> {
    if any.type_url != T::type_url() {
        return Err(format!(
            "Any holds {}, want {}",
            any.type_url,
            T::type_url()
        ));
    }
    T::decode(any.value.as_slice()).map_err(|e| format!("decoding {}: {e}", T::full_name()))
}

/// A protobuf duration.
pub fn duration(d: Duration) -> PbDuration {
    PbDuration {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

/// A protobuf duration back as a std duration (negative clamps to zero).
#[cfg(test)]
pub fn to_std(d: &PbDuration) -> Duration {
    if d.seconds < 0 || d.nanos < 0 {
        return Duration::ZERO;
    }
    Duration::new(d.seconds as u64, d.nanos as u32)
}

/// A `UInt32Value`.
pub fn u32_value(value: u32) -> UInt32Value {
    UInt32Value { value }
}

/// A `BoolValue`.
pub fn bool_value(value: bool) -> BoolValue {
    BoolValue { value }
}

/// A socket address `host:port`.
pub fn socket_address(host: &str, port: u32) -> Address {
    Address {
        address: Some(address::Address::SocketAddress(SocketAddress {
            address: host.to_string(),
            port_specifier: Some(socket_address::PortSpecifier::PortValue(port)),
            ..Default::default()
        })),
    }
}

/// A transport socket named `name` configured with `config`.
pub fn transport_socket(name: &str, config: Any) -> TransportSocket {
    TransportSocket {
        name: name.to_string(),
        config_type: Some(transport_socket::ConfigType::TypedConfig(config)),
    }
}

/// A config source delivering resources over the ADS stream.
pub fn ads_config_source() -> ConfigSource {
    ConfigSource {
        config_source_specifier: Some(config_source::ConfigSourceSpecifier::Ads(
            AggregatedConfigSource {},
        )),
        resource_api_version: ApiVersion::V3 as i32,
        ..Default::default()
    }
}

/// A data source read from `path`.
pub fn file_source(path: &str) -> DataSource {
    DataSource {
        specifier: Some(data_source::Specifier::Filename(path.to_string())),
        ..Default::default()
    }
}

/// A data source holding `s` inline.
pub fn inline_string(s: &str) -> DataSource {
    DataSource {
        specifier: Some(data_source::Specifier::InlineString(s.to_string())),
        ..Default::default()
    }
}
