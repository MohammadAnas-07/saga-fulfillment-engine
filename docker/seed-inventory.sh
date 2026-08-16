#!/usr/bin/env sh
#
# Demo stock, so `docker compose up` gives you a system you can actually order from.
#
# This runs *after* inventory-service, not before, and the retry loop is why: the
# `inventory` table is created by Hibernate at service startup (ddl-auto: update), so there
# is nothing to insert into until that has happened. Seeding from Postgres' own
# docker-entrypoint-initdb.d would run far too early.
#
# It lives here rather than in inventory-service because stock levels are operational data,
# not application logic. A real deployment would load them from wherever the business keeps
# them; inventing a seeding mechanism inside the service to make a demo work would be
# putting the demo into the product.

set -eu

export PGPASSWORD="${INVENTORY_DB_PASSWORD:-inventory_service}"
HOST="${POSTGRES_HOST:-postgres}"
USER="${INVENTORY_DB_USER:-inventory_service}"
DB="${INVENTORY_DB_NAME:-inventory_db}"

echo "Seeding demo stock into ${DB}..."

attempt=1
until psql -h "${HOST}" -U "${USER}" -d "${DB}" -v ON_ERROR_STOP=1 -c "
    INSERT INTO inventory (item_id, available_quantity, reserved_quantity, updated_at)
    VALUES
        ('MECH-KB-01', 100, 0, now()),
        ('USB-HUB-01', 50,  0, now()),
        ('DESK-MAT-01', 25, 0, now()),
        ('MON-ARM-01', 10, 0, now()),
        ('LOW-STOCK-01', 1, 0, now())
    ON CONFLICT (item_id) DO NOTHING;
" >/dev/null 2>&1; do
  if [ "${attempt}" -ge 60 ]; then
    echo "Gave up waiting for inventory-service to create the inventory table." >&2
    exit 1
  fi
  echo "  inventory table not ready yet (attempt ${attempt}); retrying..."
  attempt=$((attempt + 1))
  sleep 2
done

echo "Demo stock seeded:"
psql -h "${HOST}" -U "${USER}" -d "${DB}" -c \
    "SELECT item_id, available_quantity, reserved_quantity FROM inventory ORDER BY item_id;"
