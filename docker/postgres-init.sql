-- One database and one role per service.
--
-- Not cosmetic. ARCHITECTURE.md section 7 has each service owning its own schema, and the
-- integration suite found the concrete reason it matters: notification-service and
-- saga-orchestrator both own a table called processed_messages, and inventory-service and
-- payment-service both own processed_commands. A shared database would have them
-- overwriting each other's idempotency records — the failure would look like messages
-- being mysteriously ignored.
--
-- Each role can only reach its own database. That is the boundary made real rather than
-- merely documented.

CREATE ROLE order_service WITH LOGIN PASSWORD 'order_service';
CREATE ROLE inventory_service WITH LOGIN PASSWORD 'inventory_service';
CREATE ROLE payment_service WITH LOGIN PASSWORD 'payment_service';
CREATE ROLE notification_service WITH LOGIN PASSWORD 'notification_service';
CREATE ROLE saga_orchestrator WITH LOGIN PASSWORD 'saga_orchestrator';

CREATE DATABASE order_db OWNER order_service;
CREATE DATABASE inventory_db OWNER inventory_service;
CREATE DATABASE payment_db OWNER payment_service;
CREATE DATABASE notification_db OWNER notification_service;
CREATE DATABASE saga_db OWNER saga_orchestrator;

-- scheduler-service deliberately has no database. It holds no state of its own (section 4)
-- — its only persistence is the Redis lock, which is meant to expire.
