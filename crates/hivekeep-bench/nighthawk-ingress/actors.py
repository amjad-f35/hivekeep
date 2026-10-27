"""The benchmark's Actor fleet.

Creates N glutton Actors through hivekeep-api and warms each by POSTing /ping
through the gate router until it answers 200, the same path the load takes.
The Actors stay running for the whole session; cleanup deletes them, best
effort.

The hivekeep.api.v1 Python stubs are generated into the runner image (see
the Dockerfile) as hivekeep/api/v1/api_pb2*.py.
"""

import concurrent.futures
import time

import grpc
import requests

from hivekeep.api.v1 import api_pb2
from hivekeep.api.v1 import api_pb2_grpc

import spec as spec_mod

API_ADDRESS = "hivekeep-api.hivekeep-system.svc.cluster.local:443"
# The name hivekeep-api's serving certificate carries.
API_SERVER_NAME = "hivekeep-api.hivekeep-system.svc"
TOKEN_FILE = "/run/hivekeep-api-token/token"

# The default --hive (see the README's knob table). Actors are nh-<index>.
HIVE = "ingress-benchmark"

# The template the Actors are created from
# (workloads/manifests/glutton-template.yaml.tmpl).
TEMPLATE_HIVE = "benchmark-workloads"
TEMPLATE_NAME = "glutton"


def api_channel(auth_mode: str = "cert", address: str = API_ADDRESS) -> grpc.Channel:
    """A channel to hivekeep-api, verified against the service DNS trust
    bundle. "cert" presents the pod identity certificate; "token" a
    projected ServiceAccount token instead."""
    with open(spec_mod.CA_FILE, "rb") as f:
        ca = f.read()
    if auth_mode == "token":
        with open(TOKEN_FILE) as f:
            token = f.read().strip()
        creds = grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(root_certificates=ca),
            grpc.access_token_call_credentials(token),
        )
    elif auth_mode == "cert":
        with open(spec_mod.CRED_BUNDLE, "rb") as f:
            bundle = f.read()
        # The bundle holds the certificate chain and the key; gRPC finds each
        # by its PEM header.
        creds = grpc.ssl_channel_credentials(root_certificates=ca, private_key=bundle, certificate_chain=bundle)
    else:
        raise ValueError(f"unknown API client auth mode {auth_mode!r} (want cert or token)")
    return grpc.secure_channel(address, creds, options=[("grpc.ssl_target_name_override", API_SERVER_NAME)])


def api_stub(auth_mode: str = "cert"):
    return api_pb2_grpc.ControlStub(api_channel(auth_mode))


def _ignore_exists(call) -> None:
    try:
        call()
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.ALREADY_EXISTS:
            raise


def ensure_hive(stub, name: str = HIVE) -> None:
    _ignore_exists(lambda: stub.CreateHive(api_pb2.CreateHiveRequest(
        hive=api_pb2.Hive(metadata=api_pb2.ResourceMetadata(name=name)))))


def create_actor(stub, name: str, hive: str = HIVE) -> None:
    _ignore_exists(lambda: stub.CreateActor(api_pb2.CreateActorRequest(
        actor=api_pb2.Actor(
            metadata=api_pb2.ResourceMetadata(hive=hive, name=name),
            actor_template=api_pb2.ObjectRef(hive=TEMPLATE_HIVE, name=TEMPLATE_NAME),
        ))))


def _warm_actor(stub, router_url: str, name: str, hive: str, deadline: float) -> None:
    """Brings one Actor to serving, so the search never measures a cold
    start: resume it, then POST /ping through the router until it answers
    200 or the deadline passes. Resume errors are retried, since the API
    refuses until a worker has room."""
    ref = api_pb2.ObjectRef(hive=hive, name=name)
    session = requests.Session()
    session.verify = spec_mod.CA_FILE
    session.cert = spec_mod.CRED_BUNDLE
    last_err = "not attempted"
    while time.time() < deadline:
        try:
            stub.ResumeActor(api_pb2.ResumeActorRequest(actor=ref))
        except grpc.RpcError as e:
            last_err = f"ResumeActor: {e.code().name}"
        try:
            resp = session.post(
                f"{router_url.rstrip('/')}/ping",
                headers={spec_mod.TARGET_ACTOR_HEADER: f"{hive}/{name}"},
                data=b"",
                timeout=10,
            )
            if resp.status_code == 200:
                return
            last_err = f"POST /ping -> {resp.status_code}"
        except requests.RequestException as e:
            last_err = f"POST /ping: {e}"
        time.sleep(2)
    raise RuntimeError(f"actor {name} not warm by deadline ({last_err})")


def create_and_warm(stub, router_url: str, count: int, created: list[str], hive: str = HIVE,
                    warm_deadline_s: int = 600, parallelism: int = 16, log=print) -> list[str]:
    """Creates count Actors and warms them all; returns their names. Each
    name goes into created as soon as the Actor exists, so the caller can
    clean up a partial fleet when this raises."""
    ensure_hive(stub, hive)
    names = [f"nh-{i:03d}" for i in range(count)]
    for name in names:
        create_actor(stub, name, hive)
        created.append(name)
    log(f"Created {count} actors from {TEMPLATE_HIVE}/{TEMPLATE_NAME}")

    deadline = time.time() + warm_deadline_s
    failures: list[str] = []
    warmed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as ex:
        futures = {ex.submit(_warm_actor, stub, router_url, name, hive, deadline): name for name in names}
        for fut in concurrent.futures.as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                failures.append(futures[fut])
                log(f"Warm failed: {e}")
                continue
            warmed += 1
            if warmed % 10 == 0 or warmed == count:
                log(f"Warmed {warmed}/{count} actors")
    if failures:
        raise RuntimeError(
            f"{len(failures)}/{count} actors failed to warm within {warm_deadline_s}s: {failures[:5]}..."
        )
    return names


def cleanup(stub, names: list[str], hive: str = HIVE, log=print) -> None:
    """Deletes the Actors, best effort; never raises."""
    for name in names:
        try:
            stub.DeleteActor(api_pb2.DeleteActorRequest(actor=api_pb2.ObjectRef(hive=hive, name=name)))
        except Exception as e:
            log(f"DeleteActor({name}) failed: {e}")
