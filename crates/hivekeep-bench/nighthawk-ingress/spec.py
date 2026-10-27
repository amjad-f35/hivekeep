"""Builds Nighthawk's AdaptiveLoadSessionSpec.

The spec is assembled as a plain dict, then checked against the real
Nighthawk protos and rendered to textproto through the FileDescriptorSet
protogen/generate_descriptor.sh builds, so a misspelt field fails while the
spec is built, not inside Nighthawk.
"""

SPEC_MESSAGE = "nighthawk.adaptive_load.AdaptiveLoadSessionSpec"
OUTPUT_MESSAGE = "nighthawk.adaptive_load.AdaptiveLoadSessionOutput"

# Plugin names as Nighthawk registers them: the adaptive-load plugins use
# underscores, the request-source plugins hyphens.
STEP_CONTROLLER_PLUGIN = "nighthawk.exponential_search"
STEP_CONTROLLER_TYPE_URL = "type.googleapis.com/nighthawk.adaptive_load.ExponentialSearchStepControllerConfig"
BINARY_SCORING_PLUGIN = "nighthawk.binary_scoring"
BINARY_SCORING_TYPE_URL = "type.googleapis.com/nighthawk.adaptive_load.BinaryScoringFunctionConfig"
BUILTIN_METRICS_PLUGIN = "nighthawk.builtin"
REQUEST_SOURCE_PLUGIN = "nighthawk.in-line-options-list-request-source-plugin"
REQUEST_SOURCE_TYPE_URL = "type.googleapis.com/nighthawk.request_source.InLineOptionsListRequestSourceConfig"

# The gate router's serving name and the runner pod's projected credentials.
ROUTER_SNI = "gate-router.hivekeep-system.svc"
CA_FILE = "/run/servicedns.podcert.hivekeep.io/trust-bundle.pem"
CRED_BUNDLE = "/run/podidentity.podcert.hivekeep.io/credential-bundle.pem"

# The header the gate router routes an Actor's ingress by: "<hive>/<name>".
TARGET_ACTOR_HEADER = "hk-target-actor"

# Nighthawk's default failure predicates (all 0) end an execution at the first
# 4xx or 5xx; a measuring period must instead run to its end, so overload
# shows up as a lower success rate. All five defaults are overridden.
PERMISSIVE_FAILURE_PREDICATES = {
    "benchmark.http_4xx": "1000000000",
    "benchmark.http_5xx": "1000000000",
    "benchmark.pool_connection_failure": "1000000000",
    "benchmark.stream_resets": "1000000000",
    "requestsource.upstream_rq_5xx": "1000000000",
}

INFORMATIONAL_METRICS = (
    "attempted-rps",
    "achieved-rps",
    "send-rate",
    "latency-ns-mean",
    "latency-ns-mean-plus-2stdev",
)


def _metric_spec(name: str) -> dict:
    return {"metric_name": name, "metrics_plugin_name": BUILTIN_METRICS_PLUGIN}


def _binary_threshold(metric_name: str, lower_threshold: float | None = None,
                      upper_threshold: float | None = None) -> dict:
    config: dict = {"@type": BINARY_SCORING_TYPE_URL}
    if lower_threshold is not None:
        config["lower_threshold"] = lower_threshold
    if upper_threshold is not None:
        config["upper_threshold"] = upper_threshold
    return {
        "metric_spec": _metric_spec(metric_name),
        "threshold_spec": {"scoring_function": {"name": BINARY_SCORING_PLUGIN, "typed_config": config}},
    }


def _tls_context() -> dict:
    """mTLS to the router: the runner's pod identity as client certificate,
    the router verified against the service DNS trust bundle by its name."""
    return {
        "sni": ROUTER_SNI,
        "common_tls_context": {
            "tls_certificates": [{
                "certificate_chain": {"filename": CRED_BUNDLE},
                "private_key": {"filename": CRED_BUNDLE},
            }],
            "validation_context": {
                "trusted_ca": {"filename": CA_FILE},
                "match_typed_subject_alt_names": [{"san_type": "DNS", "matcher": {"exact": ROUTER_SNI}}],
            },
        },
    }


def build_spec_dict(
    *,
    uri: str,
    actor_names: list[str],
    hive: str,
    client_concurrency: int,
    connections: int,
    max_pending_requests: int,
    initial_total_rps: int,
    exponential_factor: float,
    measuring_period_s: int,
    convergence_deadline_s: int,
    testing_stage_duration_s: int,
    success_rate_threshold: float,
    send_rate_threshold: float | None = 0.9,
    tail_latency_slo_ms: float | None = None,
    benchmark_cooldown_s: int = 5,
) -> dict:
    """The session spec as a JSON-shaped dict.

    The step controller varies the per-event-loop requests_per_second, so
    its initial value is initial_total_rps / client_concurrency. The traffic
    template must not set a duration (the controller rejects it), and the
    controller forces open_loop on.
    """
    if not uri.startswith("https://"):
        raise ValueError(f"router URI must use https://, got {uri!r}")
    # One request variant per Actor; the request source cycles through them,
    # spreading the load across the whole fleet.
    request_variants = [
        {
            "request_method": "POST",
            "request_headers": [{
                "header": {"key": TARGET_ACTOR_HEADER, "value": f"{hive}/{name}"},
                "append_action": "OVERWRITE_IF_EXISTS_OR_ADD",
            }],
        }
        for name in actor_names
    ]
    # What each threshold guards: success-rate catches fast failures (503s,
    # 504s) that latency and send-rate miss; send-rate is the open-loop
    # backstop, since skipped requests are neither failures nor latency
    # samples and without it the search climbs without bound; the tail
    # latency (mean + 2 stdev, the closest the built-in metrics come to a
    # percentile) is the SLO.
    thresholds = [_binary_threshold("success-rate", lower_threshold=success_rate_threshold)]
    if send_rate_threshold is not None:
        thresholds.append(_binary_threshold("send-rate", lower_threshold=send_rate_threshold))
    if tail_latency_slo_ms is not None:
        thresholds.append(_binary_threshold(
            "latency-ns-mean-plus-2stdev", upper_threshold=tail_latency_slo_ms * 1_000_000.0))

    traffic_template: dict = {
        "uri": uri,
        "concurrency": str(client_concurrency),
        "connections": connections,
        "max_pending_requests": max_pending_requests,
        "open_loop": True,
        "failure_predicates": dict(PERMISSIVE_FAILURE_PREDICATES),
        # Shares a oneof with request_options: method and headers come from
        # the per-variant options.
        "request_source_plugin_config": {
            "name": REQUEST_SOURCE_PLUGIN,
            "typed_config": {
                "@type": REQUEST_SOURCE_TYPE_URL,
                "options_list": {"options": request_variants},
                # 0: cycle through the list without end.
                "num_requests": 0,
            },
        },
        "tls_context": _tls_context(),
    }
    return {
        "nighthawk_traffic_template": traffic_template,
        "metric_thresholds": thresholds,
        "informational_metric_specs": [_metric_spec(m) for m in INFORMATIONAL_METRICS],
        "step_controller_config": {
            "name": STEP_CONTROLLER_PLUGIN,
            "typed_config": {
                "@type": STEP_CONTROLLER_TYPE_URL,
                "initial_value": max(1.0, initial_total_rps / client_concurrency),
                "exponential_factor": exponential_factor,
            },
        },
        "measuring_period": f"{measuring_period_s}s",
        "convergence_deadline": f"{convergence_deadline_s}s",
        "testing_stage_duration": f"{testing_stage_duration_s}s",
        "benchmark_cooldown_duration": f"{benchmark_cooldown_s}s",
    }


# The protobuf imports below are deferred so the dict builder works without
# protobuf installed.


def load_pool(desc_path: str):
    from google.protobuf import descriptor_pb2
    from google.protobuf import descriptor_pool

    pool = descriptor_pool.DescriptorPool()
    fds = descriptor_pb2.FileDescriptorSet()
    with open(desc_path, "rb") as f:
        fds.ParseFromString(f.read())
    for file_proto in fds.file:
        pool.Add(file_proto)
    return pool


def message_class(pool, full_name: str):
    from google.protobuf import message_factory

    return message_factory.GetMessageClass(pool.FindMessageTypeByName(full_name))


def spec_dict_to_textproto(spec_dict: dict, desc_path: str) -> str:
    """Checks the dict against the real protos and renders textproto.

    MessageToString gets no descriptor pool on purpose: the expanded form of
    an Any cannot be parsed back by nighthawk_adaptive_load_client for plugin
    types it does not link (the request-source config), while the raw
    type_url/value form can.
    """
    from google.protobuf import json_format
    from google.protobuf import text_format

    pool = load_pool(desc_path)
    msg = message_class(pool, SPEC_MESSAGE)()
    json_format.ParseDict(spec_dict, msg, descriptor_pool=pool)
    return text_format.MessageToString(msg)
