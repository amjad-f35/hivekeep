#!/bin/sh
set -eu
cat > /dev/null
printf '%s\n' "{\"kind\":\"CredentialProviderResponse\",\"apiVersion\":\"credentialprovider.kubelet.k8s.io/v1\",\"cacheKeyType\":\"Registry\",\"auth\":{\"gcr.io\":{\"username\":\"$1\",\"password\":\"$2\"}}}"
