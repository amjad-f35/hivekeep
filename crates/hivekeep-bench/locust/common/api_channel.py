"""The mTLS gRPC channel to hivekeep-api used by the Python user classes.

hivekeep-api refuses unauthenticated calls: it takes the caller's identity
from an mTLS client certificate, or else from an `authorization: Bearer`
token. These classes present the pod-identity certificate the load generator
pod is issued (locust/manifests/locust.yaml projects both files).
"""

import re

import grpc

CA_FILE = "/run/servicedns.podcert.hivekeep.io/trust-bundle.pem"
CRED_BUNDLE = "/run/podidentity.podcert.hivekeep.io/credential-bundle.pem"

# The name on hivekeep-api's Service-DNS serving certificate.
SERVER_NAME = "hivekeep-api.hivekeep-system.svc"

# The default dial target, host:port.
API_HOST = "hivekeep-api.hivekeep-system.svc.cluster.local:443"

_PEM_BLOCK = re.compile(
    rb"-----BEGIN (?P<kind>[A-Z ]+)-----.*?-----END (?P=kind)-----\n?",
    re.DOTALL,
)


def split_cred_bundle(bundle: bytes) -> tuple[bytes, bytes]:
    """Splits a pod-certificate credential bundle into (key, chain).

    The bundle is one PEM file: a PRIVATE KEY block, then the CERTIFICATE
    chain from leaf to root. grpc takes the two separately. Splitting on the
    PEM armor needs no crypto library and works for any key type.
    """
    key, chain = None, []
    for m in _PEM_BLOCK.finditer(bundle):
        if m.group("kind") == b"PRIVATE KEY":
            key = m.group(0)
        else:
            chain.append(m.group(0))
    if key is None:
        raise ValueError(f"{CRED_BUNDLE}: no PRIVATE KEY block")
    if not chain:
        raise ValueError(f"{CRED_BUNDLE}: no CERTIFICATE block")
    return key, b"".join(chain)


def api_channel(host: str = API_HOST, options=None) -> grpc.Channel:
    """An mTLS channel to hivekeep-api that authenticates as this pod.

    The certificate is read when the channel is built; Python's grpc offers
    no hook to reload it per handshake. Each user builds its own channel in
    on_start, so a respawned user picks up a rotated certificate, and an
    established connection outlives the old certificate's expiry.
    """
    target = host.replace("http://", "").replace("https://", "")
    with open(CA_FILE, "rb") as f:
        ca_cert = f.read()
    with open(CRED_BUNDLE, "rb") as f:
        private_key, cert_chain = split_cred_bundle(f.read())

    creds = grpc.ssl_channel_credentials(
        root_certificates=ca_cert,
        private_key=private_key,
        certificate_chain=cert_chain,
    )
    channel_options = [("grpc.ssl_target_name_override", SERVER_NAME)]
    if options:
        channel_options.extend(options)
    return grpc.secure_channel(target, creds, options=channel_options)
