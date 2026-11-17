BEGIN;

CREATE TABLE dispatch_route (
  route_id UUID PRIMARY KEY,
  dispatch_id UUID NOT NULL,
  route_version INT NOT NULL CHECK (route_version > 0),
  route_coordinates JSONB NOT NULL,
  road_segments JSONB NOT NULL,
  estimated_duration INT NOT NULL CHECK (estimated_duration >= 0),
  change_reason VARCHAR NULL,
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (dispatch_id, route_version)
);

CREATE FUNCTION reject_dispatch_route_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'dispatch_route is append-only';
END;
$$;
CREATE TRIGGER dispatch_route_append_only BEFORE UPDATE OR DELETE ON dispatch_route
FOR EACH ROW EXECUTE FUNCTION reject_dispatch_route_mutation();

CREATE TABLE emergency_route_outbox (
  outbox_id BIGSERIAL PRIMARY KEY,
  route_id UUID NOT NULL REFERENCES dispatch_route(route_id),
  event_key VARCHAR NOT NULL UNIQUE,
  event JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  delivered_at TIMESTAMPTZ NULL,
  message_id VARCHAR NULL
);
CREATE INDEX emergency_route_outbox_pending ON emergency_route_outbox(outbox_id) WHERE delivered_at IS NULL;

COMMIT;
