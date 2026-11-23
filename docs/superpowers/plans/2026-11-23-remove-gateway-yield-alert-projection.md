# `evacs:stream:yield-alert` 임시 전달 경로 제거 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emergency Worker가 Gateway로 직접 보내던 임시 축약 알림(`evacs:stream:yield-alert`)을 없애고, 출력은 설계서대로 `stream:alert:event`(canonical 봉투) 하나만 남긴다.

**Architecture:** 지금은 `RedisStreamEventPublisher`가 Lua 스크립트 하나로 XADD를 두 번 한다. 하나는 canonical 스트림, 다른 하나는 `YIELD_ALERT_REQUESTED`일 때만 Gateway 축약본이다. 이 축약본(projection)과 관련 설정·타입·테스트 기대값을 지우고, publisher는 canonical 스트림에 XADD 한 번만 한다. 판정 로직, Redis/MySQL outbox, 멱등 키는 바꾸지 않는다.

**Tech Stack:** TypeScript 6, ioredis 6, Jest 30 (unit + integration: Redis `127.0.0.1:6401`, MySQL `127.0.0.1:33306`)

**Spec:** 별도 spec 문서는 없다. 근거는 세션 `1fdbf8ef-9a80-4ddd-8548-df59aeda6aac`의 플로우 검토 결론이다.
- 설계 플로우: EW → `stream:alert:event` → Alert Worker → `ws:vehicle:{id}` 조회 → `channel:gateway:{gatewayId}` → Gateway.
- EW → Gateway 직통(`evacs:stream:yield-alert`)은 설계에 없다. Alert Worker가 없던 11/16에 만든 임시 우회로이므로 **제거 대상**이다.
- EW 쪽 작업 항목: "우회로 출력 제거(`gatewayAlertStream`, `GatewayYieldAlertPayload`, 이중 XADD). E2E 테스트 기준도 `stream:alert:event`로 바꾼다."

## Global Constraints

- Canonical 출력 스트림 기본값 `stream:alert:event`, 필드 `eventKey` + `event`(봉투 JSON). 형식은 바꾸지 않는다.
- `ALERT_TTL_MS`(기본 `20000`)는 canonical payload의 `expiresAt` 계산에 계속 쓰므로 **유지**한다.
- `eventKey`/`eventId` 멱등성과 at-least-once 동작(outbox → receipt → cursor)은 바꾸지 않는다.
- `docs/superpowers/plans/2026-11-16-emergency-worker.md`는 당시 결정을 남긴 기록이므로 수정하지 않는다.
- EW는 `evacs:stream:yield-alert` 키를 읽지도 쓰지도 않는다. 남아 있는 키도 건드리지 않는다(삭제는 운영자가 판단).

## Review Focus

1. **배포 환경에 `EVACS_ALERT_STREAM`이 남아 있는 경우:** Worker가 에러 없이 뜨고 이 값을 무시해야 한다. → Task 1 config 테스트
2. **Redis에 이전 `evacs:stream:yield-alert` 키가 남아 있는 경우(데이터가 있거나 타입이 다름):** issue 발행이 이 키에 영향을 받지 않고 성공해야 한다. 지금은 이 키 타입이 틀리면 발행이 실패한다. → Task 1 publisher 테스트
3. **canonical 스트림 키 타입이 틀린 경우:** 일부만 쓰이지 않고 거부되어야 한다. → Task 1 publisher 테스트
4. **차량 소켓 알림이 끊긴다.** Alert Worker → Gateway 경로가 아직 끊겨 있어서, 이 작업 후에는 Gateway 작업 전까지 `yield.alert` 소켓 메시지가 안 간다. 스모크 스크립트가 소켓 수신을 통과 기준으로 쓰지 않게 바꾸고 README에 명시한다. → Task 2
5. **crash 후 재전송:** 같은 `eventKey`로 다시 나가야 한다. 기존 recovery 테스트가 canonical 스트림만 보도록 남겨서 계속 검증한다. → Task 1

---

### Task 1: publisher의 Gateway projection과 설정 제거

`gatewayAlertStream`을 지우면 이 키를 참조하는 테스트들이 컴파일되지 않는다. 그래서 코드와 테스트를 한 task에서 같이 바꾼다.

**Files:**
- Modify: `src/publishing/redis-stream-event-publisher.ts` (Lua `publishScript`, projection 분기 삭제)
- Modify: `src/publishing/redis-alert-outbox-dispatcher.ts:23-24` (config `Pick`에서 `'gatewayAlertStream'` 제거)
- Modify: `src/domain/contracts.ts` (`GATEWAY_ALERT_STREAM`(:10), env 매핑 `gatewayAlertStream: 'EVACS_ALERT_STREAM'`(:44), `GATEWAY_ALERT_FIELDS`·`GatewayYieldAlertPayload`(:136-141) 삭제)
- Modify: `src/config/config.ts:6,55` (`gatewayAlertStream` 필드·로딩 삭제)
- Test: `test/integration/publishing/redis-stream-event-publisher.test.ts`
- Test: `test/unit/config/config.test.ts`
- Test: `test/integration/decision/yield-decision-service.test.ts:81-82, 274-279`
- Test: `test/integration/worker.e2e-spec.ts:100-130, 147, 173, 237`

**Interfaces:**
- Produces: `new RedisStreamEventPublisher(redis: Redis, config: Pick<WorkerConfig, 'alertStreamMaxlen'> = loadConfig())`. `publish(stream, event): Promise<PublishReceipt>` 시그니처는 그대로다.
- Produces: `RedisAlertOutboxDispatcher`의 config 타입은 `Pick<WorkerConfig, 'alertOutboxStream' | 'alertStream' | 'readCount' | 'alertStreamMaxlen'>`.
- Produces: `WorkerConfig`에 `gatewayAlertStream`이 없다.

- [ ] **Step 1: publisher 테스트를 새 계약으로 다시 쓴다**

`describe` 이름은 `'Redis canonical publisher'`로 바꾼다. 파일 상단에 `const LEGACY_GATEWAY_STREAM = 'evacs:stream:yield-alert';`를 둔다(src의 상수는 지워지므로 리터럴로 쓴다).

```ts
test('writes a yield issue only to the canonical stream with one stable idempotency key', async () => {
  const event = request(decision()).event;
  const receipt = await new RedisStreamEventPublisher(fixture.redis, config).publish(config.alertStream, event);
  const [[id, canonical]] = await fixture.redis.xrange(config.alertStream, '-', '+') as [[string, string[]]];
  expect(receipt).toMatchObject({messageId: id, publishedAt: expect.any(String)});
  expect(parseStreamEvent(canonical)).toEqual(event);
  expect(canonical[canonical.indexOf('eventKey') + 1]).toBe(event.eventId);
  expect(await fixture.redis.exists(LEGACY_GATEWAY_STREAM)).toBe(0);
});

test('a leftover legacy Gateway key of the wrong type does not affect publishing', async () => {
  await fixture.redis.set(LEGACY_GATEWAY_STREAM, 'wrong-type');
  await new RedisStreamEventPublisher(fixture.redis, config).publish(config.alertStream, request(decision()).event);
  expect(await fixture.redis.xlen(config.alertStream)).toBe(1);
  expect(await fixture.redis.get(LEGACY_GATEWAY_STREAM)).toBe('wrong-type');
});

test('an invalid canonical destination rejects without writing', async () => {
  await fixture.redis.set(config.alertStream, 'wrong-type');
  await expect(new RedisStreamEventPublisher(fixture.redis, config).publish(config.alertStream, request(decision()).event))
    .rejects.toThrow();
  expect(await fixture.redis.get(config.alertStream)).toBe('wrong-type');
});
```

기존 clear 테스트(`publishes clear requests canonically…`)는 남긴다. 마지막 줄만 `expect(await fixture.redis.exists(LEGACY_GATEWAY_STREAM)).toBe(0)`으로 바꾼다.

- [ ] **Step 2: config 테스트를 바꾼다**

- `defaults keep canonical streams separate…` → 이름을 `'defaults use the canonical alert stream'`으로 바꾸고 기대 객체에서 `gatewayAlertStream`를 뺀다. 아래 assertion을 추가한다.
  ```ts
  expect(loadConfig({})).not.toHaveProperty('gatewayAlertStream');
  ```
- `allows explicit streams…` → 입력의 `EVACS_ALERT_STREAM: 'gateway'`는 그대로 둔다(남은 환경변수를 무시하는지 보기 위해). 기대값에서 `gatewayAlertStream: 'gateway'`를 빼고 `.not.toHaveProperty('gatewayAlertStream')`를 추가한다.

- [ ] **Step 3: 실패를 확인한다**

Run: `npx jest test/unit/config test/integration/publishing`
Expected: FAIL. config는 `gatewayAlertStream` 속성이 있어서 실패한다. publisher는 legacy 키 존재·wrong-type 테스트가 실패한다.

- [ ] **Step 4: src를 수정한다**

- `RedisStreamEventPublisher.publish`: `parseEventEnvelope(event)` 다음에 `this.redis.xadd(stream, 'MAXLEN', '~', String(this.config.alertStreamMaxlen), '*', 'eventKey', event.eventId, 'event', JSON.stringify(event))`를 호출하고 그 id를 `messageId`로 쓴다. 쓰는 키가 하나뿐이고 잘못된 타입이면 XADD가 WRONGTYPE으로 아무것도 쓰지 않으므로 Lua `publishScript`는 통째로 지운다. 클래스 주석은 `/** Transport adapter: canonical request events to the Alert Worker stream. */`로 바꾼다. `xadd`가 `null`을 돌려주면 `throw new Error('Destination XADD returned no receipt')`한다.
- dispatcher, contracts, config에서는 위 Files 목록의 항목을 지운다. `npx tsc --noEmit`이 통과할 때까지 남은 참조를 정리한다(`src/worker.ts:93`은 `this.config`를 그대로 넘기므로 수정할 필요가 없다).

- [ ] **Step 5: projection을 보던 integration 테스트를 canonical 기준으로 바꾼다**

- `yield-decision-service.test.ts:81-82`: 
  ```ts
  const delivered = (await fixture.redis.xrange(config.alertStream, '-', '+')).map(([, f]) => parseStreamEvent(f))
    .filter(event => event.eventType === 'YIELD_ALERT_REQUESTED');
  expect(delivered.map(event => (event.payload as {decisionVersion: number}).decisionVersion)).toEqual([1, 2]);
  ```
- `yield-decision-service.test.ts:274-279`: projection 검증 블록을 지운다. 바로 위의 canonical `delivered` 검증(2건, eventKey/eventId 1종)이 Review Focus 5를 계속 덮는다.
- `worker.e2e-spec.ts`
  - 테스트 이름을 `'recovers committed alert intent on restart and emits one canonical clear'`로 바꾼다.
  - :114 `xlen(config.alertStream)).toBe(0)`
  - :119-123 → `eventually(xlen(config.alertStream) === 1)` 후, 첫 엔트리를 `parseStreamEvent`해서 `toMatchObject({eventType: 'YIELD_ALERT_REQUESTED', payload: {targetVehicleId: 'target', emergencyVehicleId: 'ambulance', decisionVersion: 1}})`.
  - :128 삭제한다(:125-127이 canonical 2건을 검증한다).
  - 테스트 끝에 `expect(await fixture.redis.exists('evacs:stream:yield-alert')).toBe(0)`을 추가한다.
  - :147, :173 `config.gatewayAlertStream` → `config.alertStream`
  - :237 삭제한다.

- [ ] **Step 6: 전체 검증**

Run: `npx tsc --noEmit && npm test` (integration은 README의 Verification 절 환경변수 `REDIS_URL=redis://127.0.0.1:6401 TEST_MYSQL_ADMIN_URL=mysql://root:root@127.0.0.1:33306`로 돌린다)
Expected: 타입 에러 0, 전 스위트 PASS. 그다음 `git grep -n "gatewayAlertStream\|GatewayYieldAlert\|GATEWAY_ALERT" -- src test`의 출력이 비어 있어야 한다.

- [ ] **Step 7: Commit**

```bash
git add src test
git commit -m "refactor: stop projecting yield alerts to the Gateway stream"
```

---

### Task 2: 스모크 스크립트·배포 예시·README 정리 (이 저장소)

**Files:**
- Modify: `deploy/verify-local.cjs:60-84`
- Modify: `deploy/.env.example:21`
- Modify: `README.md:47, 92, 104, 116`

**Interfaces:**
- Consumes: Task 1의 계약. EW 출력은 `stream:alert:event`뿐이다.

- [ ] **Step 1: 스모크 스크립트의 통과 기준을 바꾼다**

`target.on('yield.alert', …)`와 `alerts` 배열, `'Gateway yield.alert delivery'` 대기, `alerts[0]` assertion 2줄을 지운다. `canonical()` 헬퍼 정의를 cone 검증 바로 뒤로 옮긴다. 그 자리에 아래 대기를 넣는다.
```js
await eventually(async () => (await canonical()).some(value => value.eventType === 'YIELD_ALERT_REQUESTED'
  && value.payload.targetVehicleId === targetVehicleId), 'canonical yield request on stream:alert:event');
```

- [ ] **Step 2: 문법 확인**

Run: `node --check deploy/verify-local.cjs`
Expected: 출력 없음, exit 0. 로컬 스택을 띄울 수 있으면 스크립트를 실제로 돌려 `"result":"PASS"`가 나오는지 확인한다. 못 돌리면 못 돌렸다고 보고한다.

- [ ] **Step 3: `.env.example`과 README를 고친다**

- `.env.example`: `EVACS_ALERT_STREAM=…` 줄을 지운다.
- `README.md:92`: `Current Gateway output` 행을 지운다.
- `README.md:47`: "receives `YIELD_ALERT_ISSUED` on the target socket"을 "reads the canonical `YIELD_ALERT_REQUESTED` for the target from `stream:alert:event`"로 바꾼다.
- `README.md:104`: 첫 두 문장을 다음으로 바꾼다. "The worker publishes only canonical requests to `stream:alert:event`; client delivery is the Alert Worker's job via `ws:vehicle:{id}` and `channel:gateway:{gatewayId}`. Until the Gateway implements that contract, vehicles receive no socket alerts from this worker." 나머지 문장은 유지한다.
- `README.md:116`: 테스트 목록의 "Gateway projection, "을 지운다.

- [ ] **Step 4: 남은 참조 확인**

Run: `git grep -n "yield-alert\|EVACS_ALERT_STREAM\|Gateway projection" -- . ':!docs/superpowers/plans/2026-11-16-emergency-worker.md'`
Expected: Task 1에서 넣은 legacy 리터럴(테스트 2곳)만 나온다.

- [ ] **Step 5: Commit**

```bash
git add deploy README.md
git commit -m "docs: drop the temporary Gateway alert stream from deploy and README"
```

---

### Task 3: 워크스페이스 공용 파일 정리 (git 밖, 커밋 없음)

`/Users/chs/dev/Inha-Capstone`은 git 저장소가 아니다. 아래 파일은 직접 수정하고 결과만 보고한다.

**Files:**
- Modify: `../README.local.md:50-60` ("알림 흐름 확인"에서 `evacs:stream:yield-alert`에 직접 XADD하는 예시), `:120-121` ("현재 Gateway 호환 payload는 … 쓴다")
- 유지: `../docker-compose.local.yml:27` (`gateway` 서비스의 `EVACS_ALERT_STREAM`)

- [ ] **Step 1: `README.local.md`를 고친다**

- :120-121: "canonical 요청은 `stream:alert:event`에만 쓴다. 차량 전달은 Alert Worker → Gateway 경로가 담당하며, Gateway가 이 계약을 구현하기 전까지 소켓 알림은 가지 않는다."
- :50-60: Gateway의 `RedisEventSubscriber`가 남아 있는 동안에는 이 수동 XADD 예시가 Gateway 단독 테스트용으로 여전히 동작한다. 그래서 지우지 않고 맨 앞에 "Gateway 단독 확인용(임시). Emergency Worker는 이 스트림에 쓰지 않는다." 한 줄만 추가한다.

- [ ] **Step 2: compose는 그대로 둔다**

`gateway` 서비스의 `EVACS_ALERT_STREAM`/`EVACS_ALERT_GROUP`은 Gateway의 `RedisEventSubscriber`가 읽는 설정이다. EW 쪽 compose 설정에는 이 변수가 없다. Gateway 쪽 작업(아래)에서 소비자를 지울 때 함께 지운다.

---

## 이 플랜 범위 밖 (후속 작업)

세션 1fdbf8ef 검토의 "설계대로 되돌리는 작업" 중 Gateway·client-core 항목은 별도 플랜으로 진행한다. 이 플랜을 머지하면 **그 작업이 끝날 때까지 차량에 양보 알림이 가지 않는다.**
- Gateway: 연결 시 `ws:vehicle:{id}` SET(종료 시 sessionId 비교 후 삭제), `channel:gateway:{GATEWAY_ID}` SUBSCRIBE, `YIELD_ALERT_ACKNOWLEDGED` → `stream:alert:event`, `RedisEventSubscriber`/`AlertMessageService` 축약 메시지 제거, compose의 `EVACS_ALERT_*` 제거.
- client-core: `YieldAlertIssued`/`YieldAlertCleared`/`EmergencyRouteUpdated` 타입을 Alert Worker `AlertMessageFactory` 출력(설계서 p106–107)에 맞춘다.
- 팀 결정: Pub/Sub + 클라이언트 ACK 재전송으로 갈지, Gateway별 Stream으로 갈지.
