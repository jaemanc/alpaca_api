# Requirements Document

## Introduction

Alpaca Markets API를 통해 실시간 주식 시세 변동 및 호가창(Level 2) 상태 변화를 감지하고, 사용자가 설정한 조건에 따라 iOS 푸시 알림 또는 이메일로 알림을 전달하는 시스템이다. Kafka와 Redis 기반의 경량 스트리밍 파이프라인으로 구성하며, 최소한의 리소스를 사용한다.

## Glossary

- **Alert_System**: 실시간 주식 알림 시스템 전체를 지칭하는 최상위 시스템
- **Data_Ingester**: Alpaca API로부터 WebSocket을 통해 실시간 시세 및 호가 데이터를 수신하는 컴포넌트
- **Alert_Engine**: 사용자 설정 조건과 수신된 시장 데이터를 비교하여 알림 발생 여부를 판단하는 컴포넌트
- **Notification_Service**: 알림 조건 충족 시 iOS 푸시 알림 또는 이메일을 발송하는 컴포넌트
- **Kafka_Pipeline**: 시장 데이터 이벤트를 비동기적으로 전달하는 메시지 브로커 파이프라인
- **Redis_Store**: 시세 캐시, 사용자 설정, 알림 상태를 저장하는 인메모리 데이터 저장소
- **Alert_Rule**: 사용자가 정의한 알림 조건 (종목, 가격 임계값, 변동률 등)
- **APNs**: Apple Push Notification service, iOS 기기에 푸시 알림을 전달하는 Apple 서비스
- **Order_Book**: 호가창 데이터, 매수/매도 호가 및 수량 정보를 포함하는 Level 2 시장 데이터
- **Quote**: 실시간 시세 데이터, 현재가/매수호가/매도호가/거래량 등을 포함

## Requirements

### Requirement 1: 실시간 시장 데이터 수신

**User Story:** 개발자로서, Alpaca API로부터 실시간 시세 및 호가 데이터를 안정적으로 수신하고 싶다. 이를 통해 시장 변동을 즉시 감지할 수 있다.

#### Acceptance Criteria

1. WHEN Alert_System이 시작되면, THE Data_Ingester SHALL 10초 이내에 Alpaca Markets WebSocket API에 연결하고 설정된 종목 목록에 대한 시세 스트림을 구독한다
2. WHEN Alpaca API로부터 Quote 데이터가 수신되면, THE Data_Ingester SHALL 해당 데이터를 500ms 이내에 Kafka_Pipeline의 `market-quotes` 토픽으로 발행한다
3. WHEN Alpaca API로부터 Order_Book 데이터가 수신되면, THE Data_Ingester SHALL 해당 데이터를 500ms 이내에 Kafka_Pipeline의 `order-book` 토픽으로 발행한다
4. IF WebSocket 연결이 끊어지면, THEN THE Data_Ingester SHALL 초기 간격 1초, 최대 간격 30초의 지수 백오프(exponential backoff) 전략으로 최대 5회 재연결을 시도한다
5. IF 재연결 5회 모두 실패하면, THEN THE Data_Ingester SHALL 에러 로그를 기록하고 관리자에게 iOS 푸시 알림(불가 시 이메일 폴백)을 발송한다
6. IF Kafka_Pipeline으로의 데이터 발행이 실패하면, THEN THE Data_Ingester SHALL 최대 3회 재시도하고, 재시도 모두 실패 시 해당 메시지를 로컬 버퍼(최대 1000건)에 보관한 후 에러 로그를 기록한다
7. IF WebSocket 연결 시 인증이 실패하면, THEN THE Data_Ingester SHALL 인증 실패를 나타내는 에러 로그를 기록하고 관리자에게 알림을 발송하며 재연결을 시도하지 않는다

### Requirement 2: Kafka 메시지 파이프라인

**User Story:** 시스템 운영자로서, 시장 데이터 이벤트를 안정적이고 순서가 보장된 방식으로 처리하고 싶다. 이를 통해 알림 누락 없이 모든 이벤트를 처리할 수 있다.

#### Acceptance Criteria

1. THE Kafka_Pipeline SHALL `market-quotes` 토픽과 `order-book` 토픽을 각각 파티션 1개, 복제 팩터 1로 운영한다
2. WHEN 메시지가 Kafka 토픽에 발행되면, THE Kafka_Pipeline SHALL 종목 심볼(ticker symbol)을 파티션 키로 사용하여 동일 종목의 메시지 순서를 보장한다
3. THE Kafka_Pipeline SHALL 메시지 보존 기간을 24시간으로 설정한다
4. IF Kafka 브로커에 연결할 수 없으면, THEN THE Alert_System SHALL 메시지를 로컬 버퍼에 최대 1000건까지 임시 저장하고, 5초 간격으로 최대 60회 재연결을 시도하며, 연결 복구 시 버퍼링된 메시지를 발행 순서대로 재전송한다
5. IF 로컬 버퍼가 최대 용량(1000건)에 도달하면, THEN THE Alert_System SHALL 가장 오래된 메시지부터 폐기하고 버퍼 초과 발생을 나타내는 경고를 기록한다
6. IF Consumer가 메시지 처리에 실패하면, THEN THE Kafka_Pipeline SHALL 최대 3회까지 1초 간격으로 재시도하고, 재시도 초과 시 해당 메시지를 데드레터 큐에 저장한다

### Requirement 3: Redis 캐시 및 상태 관리

**User Story:** 시스템 운영자로서, 최신 시세 데이터와 사용자 설정을 빠르게 조회하고 싶다. 이를 통해 알림 판단 지연을 최소화할 수 있다.

#### Acceptance Criteria

1. WHEN 새로운 Quote 데이터가 수신되면, THE Redis_Store SHALL 해당 종목의 최신 시세(현재가, 거래량, 타임스탬프)를 100ms 이내에 캐시에 갱신한다
2. THE Redis_Store SHALL 각 종목의 최신 시세 데이터에 TTL(Time To Live) 60초를 설정한다
3. THE Redis_Store SHALL 사용자별 Alert_Rule 목록을 해시 구조로 저장하며, 사용자당 최대 50개의 Alert_Rule을 허용한다
4. WHEN 알림이 발송되면, THE Redis_Store SHALL 해당 Alert_Rule의 마지막 발송 시각을 기록하고, 동일 Alert_Rule에 대해 최소 60초 이내의 재발송을 차단한다
5. THE Redis_Store SHALL 메모리 사용량을 최대 256MB로 제한하고 LRU 정책으로 오래된 캐시를 제거한다
6. IF Redis 연결이 실패하면, THEN THE Redis_Store SHALL 최대 3회 재연결을 시도하고, 모든 재시도 실패 시 에러 로그를 기록하며 시스템 운영자에게 장애 알림을 전송한다
7. IF TTL 만료로 캐시에 해당 종목의 시세 데이터가 존재하지 않으면, THEN THE Redis_Store SHALL 캐시 미스를 호출자에게 반환하여 원본 데이터 소스로부터 재조회를 유도한다

### Requirement 4: 가격 변동 감지 및 알림 규칙

**User Story:** 투자자로서, 관심 종목의 가격이 내가 설정한 조건에 도달하면 즉시 알림을 받고 싶다. 이를 통해 매매 타이밍을 놓치지 않을 수 있다.

#### Acceptance Criteria

1. WHEN Quote 데이터가 Kafka에서 소비되면, THE Alert_Engine SHALL 해당 종목에 등록된 모든 활성 Alert_Rule을 조회하여 현재가와 비교하고, 등록된 Alert_Rule이 없는 경우 추가 처리 없이 다음 메시지를 소비한다
2. IF 현재가가 사용자가 설정한 목표가 이상이면, THEN THE Alert_Engine SHALL 종목코드, 현재가, 목표가, 도달 시각, 알림 유형(상한 도달)을 포함하는 알림 이벤트를 생성한다
3. IF 현재가가 사용자가 설정한 하한가 이하이면, THEN THE Alert_Engine SHALL 종목코드, 현재가, 하한가, 도달 시각, 알림 유형(하한 도달)을 포함하는 알림 이벤트를 생성한다
4. IF 직전 거래일 종가 대비 현재가의 변동률이 사용자가 설정한 임계값(1% 이상 50% 이하 범위에서 설정 가능)을 초과하면, THEN THE Alert_Engine SHALL 종목코드, 현재가, 기준가(직전 거래일 종가), 변동률, 변동 방향(상승 또는 하락), 알림 유형(급등 또는 급락)을 포함하는 알림 이벤트를 생성한다
5. WHILE 동일 Alert_Rule에 대해 알림이 발송된 후 5분 이내인 상태에서, THE Alert_Engine SHALL 동일 조건의 중복 알림 생성을 억제한다
6. IF Quote 데이터의 가격 필드가 누락되었거나 0 이하의 값이면, THEN THE Alert_Engine SHALL 해당 메시지에 대한 알림 규칙 비교를 수행하지 않고 오류 로그를 기록한다

### Requirement 5: 호가창 상태 변화 감지

**User Story:** 투자자로서, 관심 종목의 호가창에 큰 변화가 발생하면 알림을 받고 싶다. 이를 통해 대량 매수/매도 움직임을 파악할 수 있다.

#### Acceptance Criteria

1. WHEN Order_Book 데이터가 Kafka에서 소비되면, THE Alert_Engine SHALL 직전 수신 데이터 대비 매수/매도 각 호가 레벨(1호가~10호가)의 잔량 변동 비율을 산출한다
2. WHEN 특정 호가 레벨의 잔량이 직전 수신 데이터 대비 사용자가 설정한 비율(설정 범위: 10%~500%, 기본값: 50%) 이상 변동하면, THE Alert_Engine SHALL 종목코드, 해당 호가 레벨, 변동 방향(증가/감소), 변동 비율을 포함한 호가 변동 알림 이벤트를 생성한다
3. WHEN 매수 총잔량 대비 매도 총잔량 비율이 사용자가 설정한 임계값(설정 범위: 1.5~10.0, 기본값: 3.0)을 초과하면, THE Alert_Engine SHALL 종목코드, 매수 총잔량, 매도 총잔량, 비율 값을 포함한 수급 불균형 알림 이벤트를 생성한다
4. IF 동일 종목에 대해 동일 유형의 알림 조건이 60초 이내에 재충족되면, THEN THE Alert_Engine SHALL 중복 알림을 억제하고 알림 이벤트를 생성하지 않는다

### Requirement 6: iOS 푸시 알림 발송

**User Story:** 투자자로서, 알림 조건이 충족되면 iPhone에서 즉시 푸시 알림을 받고 싶다. 이를 통해 앱을 열지 않아도 시장 상황을 파악할 수 있다.

#### Acceptance Criteria

1. WHEN 알림 이벤트가 생성되면, THE Notification_Service SHALL APNs를 통해 사용자의 등록된 모든 iOS 기기에 푸시 알림을 발송한다
2. THE Notification_Service SHALL 푸시 알림에 종목명, 현재가, 트리거된 알림 조건(예: "5% 이상 하락", "목표가 도달")을 포함한다
3. IF APNs 발송이 실패하면, THEN THE Notification_Service SHALL 5초 간격으로 최대 3회 재시도한다
4. IF APNs 재시도 3회 모두 실패하면, THEN THE Notification_Service SHALL 동일한 알림 내용을 이메일로 발송한다
5. WHEN 알림 이벤트가 생성되면, THE Notification_Service SHALL APNs 전송 요청 완료까지 1초 이내에 처리한다
6. IF APNs로부터 디바이스 토큰 무효 응답을 수신하면, THEN THE Notification_Service SHALL 해당 토큰을 비활성 처리하고 이메일 알림으로 폴백한다

### Requirement 7: 이메일 알림 발송

**User Story:** 투자자로서, 푸시 알림을 받을 수 없는 상황에서도 이메일로 알림을 받고 싶다. 이를 통해 알림을 놓치지 않을 수 있다.

#### Acceptance Criteria

1. IF 푸시 알림 발송이 실패하면(발송 후 30초 이내 전달 확인 실패 또는 발송 오류 응답 수신), THEN THE Notification_Service SHALL 사용자의 등록된 이메일 주소로 60초 이내에 알림을 발송한다
2. THE Notification_Service SHALL 이메일 본문에 종목명, 현재가, 변동률(%), 변동 방향(상승/하락), 설정된 목표 조건, 알림 시각(yyyy-MM-dd HH:mm:ss KST)을 포함한다
3. IF 이메일 발송이 실패하면, THEN THE Notification_Service SHALL 에러 로그를 기록하고 5분 후 1회 재시도한다
4. IF 재시도 후에도 이메일 발송이 실패하면, THEN THE Notification_Service SHALL 에러 로그에 최종 실패를 기록하고 해당 알림을 미전달 상태로 저장한다
5. IF 사용자의 등록된 이메일 주소가 없거나 유효하지 않은 경우, THEN THE Notification_Service SHALL 이메일 발송을 생략하고 에러 로그에 사유를 기록한다
6. WHERE 사용자가 이메일 전용 알림을 선택한 경우, THE Notification_Service SHALL 푸시 알림 없이 이메일만 발송한다

### Requirement 8: 사용자 알림 설정 관리

**User Story:** 투자자로서, 관심 종목과 알림 조건을 자유롭게 설정하고 변경하고 싶다. 이를 통해 나에게 필요한 알림만 받을 수 있다.

#### Acceptance Criteria

1. THE Alert_System SHALL 사용자별로 최대 20개의 Alert_Rule을 등록할 수 있도록 하며, 20개 초과 등록 시도 시 등록을 거부하고 상한 초과를 나타내는 오류 메시지를 반환한다
2. WHEN 사용자가 Alert_Rule을 생성하면, THE Alert_System SHALL 종목 심볼, 알림 유형(가격 도달/호가 변동), 조건값, 알림 채널(iOS 푸시, 이메일)을 저장한다
3. WHEN 사용자가 Alert_Rule을 수정하면, THE Alert_System SHALL 변경 사항을 1초 이내에 Redis_Store에 반영한다
4. WHEN 사용자가 Alert_Rule을 삭제하면, THE Alert_System SHALL 해당 규칙을 Redis_Store에서 제거하고 해당 규칙의 트리거 이력 및 쿨다운 타이머를 초기화한다
5. THE Alert_System SHALL 각 Alert_Rule에 활성/비활성 상태를 제공하여 일시적으로 알림을 중단할 수 있도록 한다
6. IF Alert_Rule 생성 시 종목 심볼이 시스템에 등록되지 않은 심볼이거나 조건값이 0 이하인 경우, THEN THE Alert_System SHALL 등록을 거부하고 유효하지 않은 필드를 나타내는 오류 메시지를 반환한다

### Requirement 9: 리소스 최적화 및 운영 제약

**User Story:** 시스템 운영자로서, 최소한의 인프라 리소스로 시스템을 안정적으로 운영하고 싶다. 이를 통해 운영 비용을 절감할 수 있다.

#### Acceptance Criteria

1. THE Alert_System SHALL 각 컨테이너의 메모리 사용량을 512MB 이하로 제한하며, Docker Compose 설정에 mem_limit을 명시한다
2. IF 컨테이너의 메모리 사용량이 512MB 제한에 도달하면, THEN THE Alert_System SHALL 해당 컨테이너를 자동 재시작하고 메모리 초과 이벤트를 로그에 기록한다
3. THE Alert_System SHALL 주기적 HTTP 폴링 없이 WebSocket 연결을 통해 실시간 데이터를 수신하며, 데이터 수신 방식은 서버 푸시 방식만 사용한다
4. THE Kafka_Pipeline SHALL 단일 브로커, 토픽당 1개 파티션 구성으로 운영한다
5. THE Alert_System SHALL Docker Compose를 통해 `docker-compose up` 단일 명령으로 전체 시스템을 배포할 수 있도록 한다
6. WHEN 시장 폐장 시간(미국 동부시간 16:00)이 도래하면, THE Data_Ingester SHALL WebSocket 연결을 해제하고 대기 모드로 전환한다
7. WHILE 대기 모드인 동안, THE Data_Ingester SHALL WebSocket 연결을 유지하지 않으며, CPU 사용률을 1% 이하로 유지한다
8. WHEN 시장 개장 시간(미국 동부시간 09:30)이 도래하면, THE Data_Ingester SHALL WebSocket 연결을 재수립하고 데이터 수신을 재개한다
