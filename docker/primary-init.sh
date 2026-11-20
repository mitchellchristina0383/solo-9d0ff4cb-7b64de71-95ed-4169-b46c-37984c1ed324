#!/bin/bash
# 主库容器首次初始化时执行（postgres 官方镜像的 docker-entrypoint-initdb.d 机制）。
# 1) 建流复制账号；2) 建库存演示库；3) 放行来自备库容器的复制连接。
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
CREATE ROLE "${REPLICATION_USER}" WITH REPLICATION LOGIN PASSWORD '${REPLICATION_PASSWORD}';
CREATE DATABASE inventory OWNER "${POSTGRES_USER}";
SQL

cat >> "$PGDATA/pg_hba.conf" <<HBA
# 流复制账号：允许 Compose 内网任意来源（仅本地演示）
host replication ${REPLICATION_USER} all scram-sha-256
HBA

echo "primary init: replication role '${REPLICATION_USER}' and database 'inventory' ready"
