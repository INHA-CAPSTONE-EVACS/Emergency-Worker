-- Emergency Worker route persistence (MySQL 8.4). Applied once, by an account allowed to create triggers
-- (root locally: binary logging is on, so trigger creation needs SUPER or log_bin_trust_function_creators).
-- Shares the Control API Server's `evacs` database. Apply after the API Server's Flyway has initialized it:
-- Flyway refuses a non-empty schema without its history table.
--
-- Types follow the API Server's conventions: UUID -> CHAR(36), timestamps -> DATETIME(3) in UTC, JSONB -> JSON.
-- dispatch_id is VARCHAR without an FK: under the API contract a dispatch is identified by its emergency ID,
-- while design-format dispatch IDs (DSP-001) are not UUIDs, and the `emergency` table belongs to the API Server.

CREATE TABLE dispatch_route (
  route_id CHAR(36) NOT NULL,
  dispatch_id VARCHAR(64) NOT NULL,
  route_version INT NOT NULL,
  route_coordinates JSON NOT NULL,
  road_segments JSON NOT NULL,
  estimated_duration INT NOT NULL,
  change_reason VARCHAR(64) NULL,
  created_at DATETIME(3) NOT NULL,
  PRIMARY KEY (route_id),
  CONSTRAINT dispatch_route_version_unique UNIQUE (dispatch_id, route_version),
  CONSTRAINT dispatch_route_version_check CHECK (route_version > 0),
  CONSTRAINT dispatch_route_duration_check CHECK (estimated_duration >= 0)
) ENGINE = InnoDB;

-- Route history is append-only (design ERD c.3). MySQL needs one trigger per statement type.
CREATE TRIGGER dispatch_route_no_update BEFORE UPDATE ON dispatch_route FOR EACH ROW
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'dispatch_route is append-only';
CREATE TRIGGER dispatch_route_no_delete BEFORE DELETE ON dispatch_route FOR EACH ROW
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'dispatch_route is append-only';

CREATE TABLE emergency_route_outbox (
  outbox_id BIGINT NOT NULL AUTO_INCREMENT,
  route_id CHAR(36) NOT NULL,
  event_key VARCHAR(255) NOT NULL,
  event JSON NOT NULL,
  created_at DATETIME(3) NOT NULL,
  delivered_at DATETIME(3) NULL,
  message_id VARCHAR(64) NULL,
  PRIMARY KEY (outbox_id),
  CONSTRAINT emergency_route_outbox_event_key_unique UNIQUE (event_key),
  CONSTRAINT emergency_route_outbox_route_fk FOREIGN KEY (route_id) REFERENCES dispatch_route (route_id)
) ENGINE = InnoDB;

-- MySQL has no partial index; delivered_at leads so the pending scan reads only the NULL range in outbox order.
CREATE INDEX emergency_route_outbox_pending ON emergency_route_outbox (delivered_at, outbox_id);
