#!/bin/bash
# Generate the acceptance cluster's mTLS material: one CA, one keypair
# per node with SAN = its compose hostname (the peer mesh's SAN
# allowlist matches [[pool]].hostname). Output: testing/.certs/.
set -euo pipefail
cd "$(dirname "$0")"

CERTS=.certs
[ -e "$CERTS/ca.crt" ] && { echo "certs: already present ($CERTS)"; exit 0; }
mkdir -p "$CERTS"

openssl genpkey -algorithm ed25519 -out "$CERTS/ca.key" 2>/dev/null
openssl req -new -x509 -key "$CERTS/ca.key" -out "$CERTS/ca.crt" \
    -days 3650 -subj "/CN=pg-agent-acceptance-ca" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign"

for node in db0 db1 db2; do
    openssl genpkey -algorithm ed25519 -out "$CERTS/$node.key" 2>/dev/null
    openssl req -new -key "$CERTS/$node.key" -out "$CERTS/$node.csr" \
        -subj "/CN=$node"
    openssl x509 -req -in "$CERTS/$node.csr" -out "$CERTS/$node.crt" \
        -CA "$CERTS/ca.crt" -CAkey "$CERTS/ca.key" -CAcreateserial \
        -days 3650 \
        -extfile <(printf '%s\n' \
            "subjectAltName=DNS:$node" \
            "keyUsage=critical,digitalSignature" \
            "extendedKeyUsage=serverAuth,clientAuth" \
            "basicConstraints=CA:FALSE")
    rm "$CERTS/$node.csr"
done
rm -f "$CERTS/ca.srl"
echo "certs: generated under $CERTS"
