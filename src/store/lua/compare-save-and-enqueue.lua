-- KEYS: dispatch hash, decision map, durable outbox, cone, then the source state hashes.
-- ARGV: expected version, next map, records, optional expected cone snapshot, expected source versions, optional route context.
-- Validate every fallible input/key before the first mutation: Lua errors do not roll back Redis writes.
local function require_type(key, expected)
  local actual = redis.call('TYPE', key).ok
  if actual ~= 'none' and actual ~= expected then error('Invalid Redis key type: ' .. key) end
end
require_type(KEYS[1], 'hash')
require_type(KEYS[2], 'string')
require_type(KEYS[3], 'stream')
local current = tonumber(redis.call('HGET', KEYS[1], 'decisionVersion') or '0')
local result = {accepted = false, decisionVersion = current, added = {}, changed = {}, removed = {}, outboxIds = {}}
if redis.call('EXISTS', KEYS[1]) == 0 or current ~= tonumber(ARGV[1]) then return cjson.encode(result) end
local status = redis.call('HGET', KEYS[1], 'status')
if status == 'COMPLETED' or status == 'CANCELED' then return cjson.encode(result) end
-- A cone move can be a decision no-op, so decisionVersion alone cannot fence stale evaluations.
if ARGV[4] ~= '' then
  require_type(KEYS[4], 'string')
  if redis.call('GET', KEYS[4]) ~= ARGV[4] then return cjson.encode(result) end
end
if ARGV[6] ~= '' then
  local expected = cjson.decode(ARGV[6])
  local route_version = tonumber(redis.call('HGET', KEYS[1], 'routeVersion') or '0')
  local destination = cjson.decode(redis.call('HGET', KEYS[1], 'destination'))
  if route_version ~= expected.routeVersion or destination.latitude ~= expected.destination.latitude
    or destination.longitude ~= expected.destination.longitude then return cjson.encode(result) end
end
-- Location writes the state and advances its monotonic version in the same transaction.
-- A newer source version can leave the decision map unchanged or recreate an expired hash.
local source_versions = cjson.decode(ARGV[5])
for i, expected in ipairs(source_versions) do
  local version = redis.call('HGET', KEYS[i + 4], 'stateVersion') or cjson.null
  if version ~= expected then return cjson.encode(result) end
end
local previous = cjson.decode(redis.call('GET', KEYS[2]) or '{}')
local next = cjson.decode(ARGV[2])
local records = cjson.decode(ARGV[3])
local semantic_fields = {'alertId', 'emergencyId', 'dispatchId', 'emergencyVehicleId', 'targetVehicleId',
  'approachDirection', 'estimatedArrivalSeconds', 'recommendedAction', 'reason'}
local function differs(a, b)
  for _, field in ipairs(semantic_fields) do if a[field] ~= b[field] then return true end end
  return false
end
local required = {}
local delta_count = 0
for id, decision in pairs(next) do
  local old = previous[id]
  if not old or differs(old, decision) then
    if decision.decisionVersion ~= current + 1 then error('Invalid next decision version') end
    if old and old.alertId ~= decision.alertId then error('Alert ID must remain stable') end
    table.insert(old and result.changed or result.added, decision)
    required[id] = {eventType = 'YIELD_ALERT_REQUESTED', decision = decision}
    delta_count = delta_count + 1
  else next[id] = old end
end
for id, old in pairs(previous) do
  if not next[id] then
    required[id] = {eventType = 'YIELD_ALERT_CLEAR_REQUESTED', decision = old}
    delta_count = delta_count + 1
  end
end
if #records ~= delta_count then error('Every decision delta requires exactly one outbox event') end
for _, record in ipairs(records) do
  local event = record.event
  local payload = event.payload
  local change = required[payload.targetVehicleId]
  if not change or change.eventType ~= event.eventType or payload.alertId ~= change.decision.alertId then
    error('Outbox event does not match decision delta')
  end
  if event.eventType == 'YIELD_ALERT_REQUESTED' then
    for field, value in pairs(payload) do if change.decision[field] ~= value then error('Request payload mismatch') end end
  else table.insert(result.removed, {decision = change.decision, reason = payload.reason}) end
  required[payload.targetVehicleId] = nil
end
result.accepted = true
if delta_count == 0 then return cjson.encode(result) end
-- Event JSON is pre-encoded before writes so no serialization failure can strand state.
local encoded = {}
for _, record in ipairs(records) do table.insert(encoded, cjson.encode(record.event)) end
redis.call('SET', KEYS[2], cjson.encode(next))
redis.call('HSET', KEYS[1], 'decisionVersion', current + 1)
for index, record in ipairs(records) do
  table.insert(result.outboxIds, redis.call('XADD', KEYS[3], '*', 'eventKey', record.eventKey, 'event', encoded[index]))
end
result.decisionVersion = current + 1
return cjson.encode(result)
