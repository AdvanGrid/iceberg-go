#!/usr/bin/env bash
# Brings up the differential lane and provisions Garage, Lakekeeper and the
# pinned DuckDB extensions. Run from the repository root: make advangrid-setup.
set -euo pipefail

compose=(docker compose -f internal/recipe/docker-compose.yml -f advangrid/recipe/docker-compose.yml)
key_id=GK26280000000000000000000a
secret=2628000000000000000000000000000000000000000000000000000000000000
catalog=http://localhost:8282

"${compose[@]}" up -d --build --wait spark-iceberg garage lakekeeper spark-oracle

garage() { "${compose[@]}" exec -T garage /garage "$@"; }
node=$(garage node id -q | cut -d@ -f1)
garage layout assign -z dc1 -c 10G "$node" >/dev/null
garage layout apply --version 1 >/dev/null 2>&1 || true
garage key info "$key_id" >/dev/null 2>&1 || garage key import --yes -n lane "$key_id" "$secret" >/dev/null
garage bucket create lake >/dev/null 2>&1 || true
garage bucket allow --read --write --owner lake --key "$key_id" >/dev/null

curl -fsS -X POST "$catalog/management/v1/bootstrap" -H 'content-type: application/json' \
  -d '{"accept-terms-of-use": true}' >/dev/null 2>&1 || true

create_warehouse() {
  local name=$1 endpoint=$2
  if curl -fsS "$catalog/management/v1/warehouse" | grep -q "\"name\":\"$name\""; then
    return
  fi
  curl -fsS -X POST "$catalog/management/v1/warehouse" -H 'content-type: application/json' -d @- >/dev/null <<JSON
{"warehouse-name":"$name","project-id":"00000000-0000-0000-0000-000000000000",
 "storage-profile":{"type":"s3","bucket":"lake","key-prefix":"$name","region":"us-east-1",
   "endpoint":"$endpoint","path-style-access":true,"sts-enabled":false,"flavor":"s3-compat",
   "remote-signing-enabled":false},
 "storage-credential":{"type":"s3","credential-type":"access-key","aws-access-key-id":"$key_id","aws-secret-access-key":"$secret"}}
JSON
}
create_warehouse lake http://localhost:3900
# Endpoint that resolves inside the stack but not on the host, like Garage's
# in-network name on the Compose lake.
create_warehouse split http://spark-iceberg:3900

ext_dir=advangrid/.duckdb/extensions/v2.0.0/linux_amd64
mkdir -p "$ext_dir"
while read -r sum file; do
  name=${file%.gz}
  if [ -f "$ext_dir/$name" ]; then
    continue
  fi
  curl -fsSL -o "$ext_dir/$file" "http://extensions.duckdb.org/v2.0.0/linux_amd64/$file"
  echo "$sum  $ext_dir/$file" | sha256sum -c --quiet -
  gunzip -f "$ext_dir/$file"
done < advangrid/recipe/duckdb-extensions.sha256

echo "advangrid lane ready"
