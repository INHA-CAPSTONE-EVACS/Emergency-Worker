# WebSocket Gateway: 경로 업데이트 Redis Stream 소비 가이드

`Emergency Worker`는 모든 `ROUTE_UPDATE_REQUESTED` 이벤트를 Redis Stream `stream:evacs_route:update`에 발행합니다. WebSocket Gateway는 Redis 소비자 그룹으로 이 스트림을 소비하고 대상 차량에는 가장 최신 경로만 전달해야 합니다.

기존 Pub/Sub 경로 구독 코드는 레거시로 남겨 둡니다. 새 경로 전달 기능을 Pub/Sub 경로에 추가하지 마십시오. 이 문서는 Stream 소비 방식만 설명합니다.

## 1. 계약

| 항목 | 값 |
| --- | --- |
| 스트림 | `stream:evacs_route:update` |
| Gateway 환경 변수 | `EVACS_ROUTE_UPDATE_STREAM=stream:evacs_route:update` |
| 소비자 그룹 | Gateway 배포 단위에서 공유하는 그룹. 예: `ws-gateway-route` |
| 소비자 이름 | 프로세스마다 고유한 이름. 예: `ws-route-<hostname>-<pid>` |
| 이벤트 타입 | `ROUTE_UPDATE_REQUESTED`만 전달됨 |
| 전달 보장 | At-least-once. 중복 전달은 정상 동작임 |
| 순서 보장 범위 | Redis 엔트리 순서만 보장됨. 같은 `dispatchId`의 이전 경로는 `routeVersion`으로 차단해야 함 |

Worker는 양보 알림 이벤트와 함께 같은 경로 이벤트를 `stream:alert:event`에도 발행합니다. Gateway 경로 소비자는 혼합 알림 스트림이 아니라 `stream:evacs_route:update`를 읽어야 합니다.

## 2. Redis 엔트리 형식

각 엔트리는 아래 애플리케이션 필드를 가집니다.

```text
eventKey = <변하지 않는 이벤트 식별자>
event    = <JSON으로 인코딩된 표준 이벤트 envelope>
```

`XRANGE stream:evacs_route:update - +` 결과의 필드 예시입니다.

```json
{
  "eventKey": "dispatch-42:route:3:ROUTE_UPDATE_REQUESTED:ambulance-7",
  "event": "{\"eventId\":\"dispatch-42:route:3:ROUTE_UPDATE_REQUESTED:ambulance-7\",\"eventType\":\"ROUTE_UPDATE_REQUESTED\",\"schemaVersion\":1,\"occurredAt\":\"2026-11-23T09:00:00.000Z\",\"producer\":\"emergency-worker\",\"payload\":{\"dispatchId\":\"dispatch-42\",\"targetVehicleId\":\"ambulance-7\",\"routeVersion\":3,\"destination\":{\"latitude\":37.451,\"longitude\":126.656},\"waypoints\":[{\"latitude\":37.452,\"longitude\":126.657}],\"congestedSegments\":[\"edge-a\"],\"estimatedTravelSeconds\":95}}"
}
```

파싱 대상은 `event` JSON 필드입니다. 이전의 평면 `payload` 필드 형식이 아닙니다. 따라서 `fields.payload`를 읽거나 Redis 엔트리 ID를 업무 멱등성 키로 사용하면 안 됩니다. 중복 제거에는 `eventKey`를 사용하며, 이 값은 `event.eventId`와 같습니다.

## 3. 표준 이벤트 스키마

```ts
interface RouteUpdateEvent {
  eventId: string;
  eventType: 'ROUTE_UPDATE_REQUESTED';
  schemaVersion: 1;
  occurredAt: string; // 밀리초 단위 ISO-8601 UTC
  producer: 'emergency-worker';
  payload: {
    dispatchId: string;
    targetVehicleId: string;
    routeVersion: number; // 양의 정수, 하나의 dispatch에서 증가
    destination: {latitude: number; longitude: number};
    waypoints: Array<{latitude: number; longitude: number}>; // 비어 있지 않음
    congestedSegments: string[];
    estimatedTravelSeconds: number; // 0 이상의 정수
  };
}
```

디코딩할 수 없거나, 지원하지 않는 `schemaVersion`이거나, `ROUTE_UPDATE_REQUESTED`가 아닌 메시지는 기록 후 ACK합니다. 가능한 경우 Redis 엔트리 ID와 `eventKey`를 함께 로그에 남깁니다. Gateway에 운영 오류 채널이 있으면 손상 메시지의 세부 정보를 남깁니다.

## 4. 소비자 그룹 생명주기

읽기 루프를 시작하기 전에 그룹을 생성합니다.

```text
XGROUP CREATE stream:evacs_route:update ws-gateway-route $ MKSTREAM
```

`BUSYGROUP` 오류는 무시합니다. `$`는 새로 배포된 Gateway가 이후에 발행되는 이벤트만 받도록 합니다. 보관된 모든 이벤트를 새 그룹으로 재생해야 할 때만 `0`을 사용합니다.

공유 그룹 및 고유 소비자 이름으로 새 메시지를 읽습니다.

```text
XREADGROUP GROUP ws-gateway-route ws-route-<instance> COUNT 10 BLOCK 5000 \
  STREAMS stream:evacs_route:update >
```

`XACK`용 일반 Redis 연결과 `XREADGROUP` 블로킹 호출용 별도 연결을 사용합니다. 종료 시에는 블로킹 연결을 닫아 루프를 즉시 깨우는 방식이 좋습니다.

## 5. 처리 및 ACK 정책

각 Redis 엔트리를 다음 순서로 처리합니다.

1. 필드 맵을 만들고 `eventKey`를 읽은 뒤 `event` JSON을 파싱합니다.
2. envelope 및 경로 payload를 검증합니다.
3. 외부에 보이는 작업 전 `eventKey`로 중복 여부를 확인합니다.
4. `payload.dispatchId`의 최신 저장 버전과 `payload.routeVersion`을 비교합니다.
5. 더 최신 이벤트면 경로 상태를 갱신하고 `targetVehicleId`에 Gateway 경로 메시지를 전송합니다.
6. 성공한 멱등성 처리 기록과 최신 경로 버전을 저장합니다.
7. 필요한 로컬 저장 및 소켓 전달 작업이 모두 성공한 뒤에만 `XACK`합니다.

At-least-once 전달 특성상 부분 실패 후 같은 이벤트가 재발행될 수 있습니다. Gateway는 재시도가 안전하도록 구현해야 합니다.

- `eventKey`가 이미 완료되었으면 외부 전송을 건너뛰고 `XACK`합니다.
- 같은 `dispatchId`의 저장된 버전보다 `routeVersion`이 작으면 건너뛰고 `XACK`합니다.
- 같은 버전이 다시 오면 저장된 `eventKey`가 다른 경우에 대한 별도 충돌 정책이 없는 한 중복으로 처리합니다.
- 경로 상태와 전달 영수증을 원자적으로 저장할 수 없다면, ACK를 앞당기지 말고 재전달을 허용합니다.

일시적인 Redis, 데이터베이스, WebSocket 전달 실패에는 `XACK`하지 않습니다. 엔트리를 PEL(Pending Entries List)에 남겨 재시도합니다. 영구적으로 잘못된 엔트리는 로그를 남기고 Gateway의 실패 기록/DLQ가 있으면 전송한 다음 `XACK`하여 복구를 막지 않게 합니다.

소켓 전달만으로 차량 클라이언트의 실제 수신을 증명할 수는 없습니다. 클라이언트 프로토콜에 별도 수신 확인이 없다면, 로컬 `emit` 또는 전송 큐 적재 성공을 Gateway의 전달 완료 경계로 봅니다.

## 6. 중단된 소비자의 PEL 복구

블로킹 읽기 전에, 또는 짧은 주기 타이머에서 종료된 Gateway 인스턴스가 남긴 PEL 엔트리를 회수합니다.

```text
XAUTOCLAIM stream:evacs_route:update ws-gateway-route ws-route-<instance> 30000 0-0 COUNT 10
```

회수한 엔트리도 새 엔트리와 같은 검증, 멱등성, ACK 규칙으로 처리합니다. 반환된 커서를 유지하고 `0-0`이 나올 때까지 이어서 회수합니다. 이후 다음 스윕에서도 저장한 커서부터 재개합니다. 정상 전달 지연보다 큰 `min-idle-time`을 선택해야 살아 있는 소비자의 처리 중인 엔트리를 가로채지 않습니다.

`XPENDING stream:evacs_route:update ws-gateway-route`를 모니터링합니다. PEL 증가, 반복 전달 또는 회수 기준보다 오래된 엔트리가 있으면 조사해야 합니다.

## 7. TypeScript 파싱 예시

```ts
type StreamEntry = [id: string, fields: string[]];

function parseRouteUpdate(entryId: string, fields: string[]): {eventKey: string; event: RouteUpdateEvent} {
  const values = new Map<string, string>();
  for (let index = 0; index + 1 < fields.length; index += 2) {
    const key = fields[index]!;
    if (values.has(key)) throw new Error(`duplicate Redis field: ${key}`);
    values.set(key, fields[index + 1]!);
  }

  const eventKey = values.get('eventKey');
  const rawEvent = values.get('event');
  if (!eventKey || !rawEvent) throw new Error(`invalid route entry: ${entryId}`);

  const event = JSON.parse(rawEvent) as RouteUpdateEvent;
  if (event.eventId !== eventKey || event.eventType !== 'ROUTE_UPDATE_REQUESTED' || event.schemaVersion !== 1) {
    throw new Error(`unsupported route event: ${entryId}`);
  }
  if (!event.payload.dispatchId || !event.payload.targetVehicleId || !Number.isSafeInteger(event.payload.routeVersion)
    || event.payload.routeVersion < 1 || !Array.isArray(event.payload.waypoints) || !event.payload.waypoints.length) {
    throw new Error(`invalid route payload: ${entryId}`);
  }
  return {eventKey, event};
}

async function processEntry(entry: StreamEntry): Promise<void> {
  const [entryId, fields] = entry;
  const {eventKey, event} = parseRouteUpdate(entryId, fields);

  if (await routeReceiptStore.has(eventKey)) {
    await redis.xack(routeStream, routeGroup, entryId);
    return;
  }

  const currentVersion = await routeStateStore.versionFor(event.payload.dispatchId);
  if (currentVersion !== undefined && event.payload.routeVersion <= currentVersion) {
    await routeReceiptStore.record(eventKey);
    await redis.xack(routeStream, routeGroup, entryId);
    return;
  }

  await routeStateStore.save(event.payload.dispatchId, event.payload.routeVersion, event.payload);
  await wsMessageSender.sendRoute(event.payload.targetVehicleId, event.payload);
  await routeReceiptStore.record(eventKey);
  await redis.xack(routeStream, routeGroup, entryId);
}
```

`routeStateStore`와 `routeReceiptStore`는 Gateway의 영속 저장소 구현으로 대체합니다. 경로 상태를 메모리에만 보관한다면 재시작 후 중복 전송을 허용해야 하며, 선택한 전달 완료 경계가 성공하기 전에는 ACK하면 안 됩니다.

## 8. 설정 및 전환 확인 목록

```dotenv
EVACS_ROUTE_UPDATE_STREAM=stream:evacs_route:update
EVACS_ROUTE_UPDATE_GROUP=ws-gateway-route
EVACS_ROUTE_UPDATE_CONSUMER_NAME=ws-route-<unique-instance-name>
EVACS_ROUTE_UPDATE_BLOCK_MS=5000
EVACS_ROUTE_UPDATE_CLAIM_IDLE_MS=30000
```

- 기존 Pub/Sub 코드는 남기되, 새 경로 전달 시작 경로에서는 비활성화합니다.
- Gateway 인스턴스마다 고유한 소비자 이름으로 Stream 소비자를 시작합니다.
- `XINFO GROUPS stream:evacs_route:update`에서 의도한 그룹이 생성됐는지 확인합니다.
- 경로를 발행하고 Gateway 처리 후 PEL 엔트리가 ACK되는지 확인합니다.
- 처리 중인 Gateway 인스턴스를 중지한 뒤, 다른 인스턴스가 `XAUTOCLAIM`으로 PEL 엔트리를 회수하는지 확인합니다.
- 같은 `eventKey`를 두 번 발행하고 이전 `routeVersion`도 발행하여, 최신 경로를 덮어쓰거나 다시 전송하지 않는지 확인합니다.

## 9. 책임 경계

`Emergency Worker`는 경로 계산, 발행 전 이벤트 검증, 스트림에 대한 at-least-once 발행을 담당합니다. WebSocket Gateway는 소비자 그룹 복구, 멱등성, 이전 버전 차단, 소켓 메시지 형식, 클라이언트 전달 동작을 담당합니다.
