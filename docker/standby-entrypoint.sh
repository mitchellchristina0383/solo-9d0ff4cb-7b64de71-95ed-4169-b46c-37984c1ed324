#!/bin/bash
# 备库容器入口：首次启动时用 pg_basebackup 从主库克隆，之后直接以流复制备库启动。
# 依赖环境变量：
#   PRIMARY_HOST / PRIMARY_PORT / REPLICATION_USER / REPLICATION_PASSWORD
set -euo pipefail

# 官方 postgres 镜像默认以 root 启动；数据卷可能要 chown，随后切到 postgres 用户操作。
if [ "$(id -u)" = "0" ]; then
  mkdir -p "$PGDATA"
  chown -R postgres:postgres "$PGDATA"
  chmod 700 "$PGDATA"
  exec gosu postgres "$0" "$@"
fi

PRIMARY_HOST="${PRIMARY_HOST:-db-primary}"
PRIMARY_PORT="${PRIMARY_PORT:-5432}"
REPLICATION_USER="${REPLICATION_USER:-replicator}"
REPLICATION_PASSWORD="${REPLICATION_PASSWORD:-replicator}"

if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "standby: 等待主库 ${PRIMARY_HOST}:${PRIMARY_PORT} ..."
  export PGPASSWORD="$REPLICATION_PASSWORD"
  until pg_isready -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -U "$REPLICATION_USER" >/dev/null 2>&1; do
    sleep 1
  done

  echo "standby: pg_basebackup 克隆主库 ..."
  # 主库健康检查通过 ≠ 复制账号已就绪（初始化期间的临时实例也会应答），
  # 所以克隆失败就清空重试，直到主库完全初始化。
  attempt=0
  until pg_basebackup \
      -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" \
      -U "$REPLICATION_USER" \
      -D "$PGDATA" -Fp -Xs -R -P; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 60 ]; then
      echo "standby: pg_basebackup 连续失败 ${attempt} 次，放弃" >&2
      exit 1
    fi
    echo "standby: 克隆失败，2 秒后重试（第 ${attempt} 次）..." >&2
    rm -rf "${PGDATA:?}"/*
    sleep 2
  done

  # -R 写出的 primary_conninfo 不含密码；~/.pgpass 里放一份，重启后仍能自动重连。
  chmod 600 "$PGDATA/postgresql.auto.conf"
  printf '%s\n' "${PRIMARY_HOST}:${PRIMARY_PORT}:replication:${REPLICATION_USER}:${REPLICATION_PASSWORD}" \
    > "$HOME/.pgpass"
  chmod 600 "$HOME/.pgpass"
  # 卷根目录可能由 Docker 以 0755 创建；postgres 要求数据目录 0700/0750。
  chmod 700 "$PGDATA"
  touch "$PGDATA/standby.signal"
  echo "standby: 克隆完成，以备库身份启动"
fi

exec docker-entrypoint.sh postgres
