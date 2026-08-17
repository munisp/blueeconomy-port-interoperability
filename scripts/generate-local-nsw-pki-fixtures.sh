#!/usr/bin/env bash
# LOCAL_CONTRACT_FIXTURE only. Never submit generated keys/certificates to NSW/PCS.
set -euo pipefail
out=${1:-fixtures/local_nsw_contract}
mkdir -p "$out"
openssl req -x509 -newkey rsa:3072 -nodes -days 7 -subj '/CN=LOCAL_CONTRACT_FIXTURE_CA' -keyout "$out/ca.key" -out "$out/ca.crt"
openssl req -newkey rsa:2048 -nodes -subj '/CN=local-nsw-fixture' -keyout "$out/server.key" -out "$out/server.csr"
printf 'subjectAltName=DNS:local-nsw-fixture\nextendedKeyUsage=serverAuth\n' > "$out/server.ext"
openssl x509 -req -in "$out/server.csr" -CA "$out/ca.crt" -CAkey "$out/ca.key" -CAcreateserial -days 7 -extfile "$out/server.ext" -out "$out/server.crt"
openssl req -newkey rsa:2048 -nodes -subj '/CN=local-s1-client' -keyout "$out/client.key" -out "$out/client.csr"
printf 'extendedKeyUsage=clientAuth\n' > "$out/client.ext"
openssl x509 -req -in "$out/client.csr" -CA "$out/ca.crt" -CAkey "$out/ca.key" -CAcreateserial -days 7 -extfile "$out/client.ext" -out "$out/client.crt"
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$out/jws-private.pem"
openssl rsa -in "$out/jws-private.pem" -pubout -out "$out/jws-public.pem"
printf '%s\n' 'LOCAL_CONTRACT_FIXTURE: use only for mTLS failure and JWS verification pipeline tests.' > "$out/README.txt"
