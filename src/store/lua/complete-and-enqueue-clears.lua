-- All completion clear records are derived from persisted decisions inside this one atomic operation.
-- KEYS: dispatch hash, cone, decision map, outbox. ARGV: dispatch ID, occurredAt.
local outbox_type = redis.call('TYPE', KEYS[4]).ok
if outbox_type ~= 'none' and outbox_type ~= 'stream' then error('Invalid outbox key type') end
local version = tonumber(redis.call('HGET', KEYS[1], 'decisionVersion') or '0') + 1
local decisions = cjson.decode(redis.call('GET', KEYS[3]) or '{}')
local targets = {}
for target, _ in pairs(decisions) do table.insert(targets, target) end
table.sort(targets)
local records = {}
for _, target in ipairs(targets) do
  local decision = decisions[target]
  local event_key = ARGV[1] .. ':' .. version .. ':YIELD_ALERT_CLEAR_REQUESTED:' .. decision.targetVehicleId
  local event = {eventId = event_key, eventType = 'YIELD_ALERT_CLEAR_REQUESTED', schemaVersion = 1,
    occurredAt = ARGV[2], producer = 'emergency-worker', payload = {alertId = decision.alertId,
      dispatchId = ARGV[1], targetVehicleId = decision.targetVehicleId, reason = 'EMERGENCY_COMPLETED'}}
  table.insert(records, {eventKey = event_key, event = cjson.encode(event)})
end
local ids = {}
for _, record in ipairs(records) do
  table.insert(ids, redis.call('XADD', KEYS[4], '*', 'eventKey', record.eventKey, 'event', record.event))
end
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
return ids
