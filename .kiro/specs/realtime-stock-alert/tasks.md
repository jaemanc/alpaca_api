# Implementation Plan: Realtime Stock Alert System

## Overview

Alpaca Markets API 기반 실시간 주식 알림 시스템을 Python 3.11+ asyncio로 구현한다. Kafka + Redis 경량 스트리밍 파이프라인으로 구성하며, Docker Compose 단일 배포를 목표로 한다. 모든 코드에 상세한 한글 주석을 포함하고, wave 단위 통합 검증만 수행한다.

## Tasks

- [ ] 1. 프로젝트 구조 및 기반 설정 (Wave 1)
  - [ ] 1.1 프로젝트 디렉토리 구조 및 설정 파일 생성
    - `services/` 하위에 `data_ingester/`, `alert_engine/`, `notification_service/` 디렉토리 생성
    - 각 서비스에 `__init__.py`, `main.py`, `Dockerfile` 생성
    - 루트에 `requirements.txt`, `.env.example`, `pyproject.toml` 생성
    - 공통 모듈 `shared/` 디렉토리에 설정 및 유틸리티 배치
    - 모든 파일에 한글 주석으로 목적 설명
    - _Requirements: 9.5_

  - [ ] 1.2 Docker Compose 설정 파일 작성
    - `docker-compose.yml` 작성 (Zookeeper, Kafka, Redis, 3개 서비스 컨테이너)
    - 각 컨테이너 mem_limit 설정 (Kafka/서비스: 512MB, Redis/Zookeeper: 256MB)
    - 네트워크 `alert-net` 구성, depends_on 의존성 설정
    - 환경변수 `.env` 파일 참조 설정
    - Redis: `--maxmemory 256mb --maxmemory-policy allkeys-lru` 커맨드
    - _Requirements: 9.1, 9.4, 9.5_

  - [ ] 1.3 데이터 모델 정의 (shared/models.py)
    - `Quote`, `OrderBookLevel`, `OrderBook`, `AlertRule`, `AlertEvent` dataclass 정의
    - `AlertType`, `NotificationChannel` Enum 정의
    - JSON 직렬화/역직렬화 메서드 (`to_json()`, `from_json()`) 구현
    - 모든 필드에 한글 주석으로 용도 설명
    - _Requirements: 1.2, 1.3, 4.1, 5.1_

  - [ ] 1.4 공통 설정 모듈 작성 (shared/config.py)
    - 환경변수 기반 설정 로드 (Alpaca API 키, Kafka/Redis 호스트, SMTP 설정)
    - Kafka 토픽명, 파티션 수, 보존 기간 상수 정의
    - Redis 키 패턴, TTL 값 상수 정의
    - 쿨다운 시간(가격: 300초, 호가: 60초) 상수 정의
    - 로깅 설정 (포맷, 레벨)
    - _Requirements: 2.1, 2.3, 3.2, 3.5_

- [ ] 2. Kafka 파이프라인 및 Redis 저장소 설정 (Wave 2)
  - [ ] 2.1 Kafka Producer/Consumer 래퍼 구현 (shared/kafka_client.py)
    - `confluent-kafka` 기반 비동기 Producer 래퍼 (asyncio.to_thread 활용)
    - Consumer 래퍼 (poll → asyncio.to_thread 비동기 래핑)
    - 파티션 키를 종목 심볼로 설정하는 발행 메서드
    - 발행 실패 시 최대 3회 재시도 로직
    - Dead Letter Queue 전송 메서드
    - 한글 주석으로 각 메서드 동작 설명
    - _Requirements: 2.1, 2.2, 2.6_

  - [ ] 2.2 로컬 메시지 버퍼 구현 (shared/buffer.py)
    - 최대 1000건 제한 순환 버퍼 (collections.deque 활용)
    - 버퍼 초과 시 가장 오래된 메시지 폐기 + 경고 로그
    - Kafka 연결 복구 시 버퍼 메시지 순서대로 재전송 메서드
    - 버퍼 상태 조회 (현재 크기, 가용 공간)
    - _Requirements: 1.6, 2.4, 2.5_

  - [ ] 2.3 Redis 클라이언트 래퍼 구현 (shared/redis_client.py)
    - `redis-py` 비동기 클라이언트 초기화 (연결 풀)
    - 시세 캐시 CRUD (TTL 60초 자동 설정)
    - Alert_Rule 해시 구조 CRUD (사용자별 저장/조회/삭제)
    - 쿨다운 상태 관리 (Redis TTL 기반 SET/GET)
    - 디바이스 토큰 및 이메일 주소 관리
    - 연결 실패 시 최대 3회 재시도 + 에러 로그
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

- [ ] 3. Checkpoint - Wave 1~2 검증
  - pytest로 import 에러, 타입 에러 부재 확인
  - 데이터 모델 필수 필드 존재 여부 체크
  - Kafka/Redis 클라이언트 인스턴스 생성 가능 여부 확인
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 4. Data_Ingester 구현 (Wave 3)
  - [ ] 4.1 Alpaca WebSocket 클라이언트 구현 (data_ingester/websocket_client.py)
    - `alpaca-py` SDK의 `StockDataStream` 활용 WebSocket 연결
    - Quote 데이터 수신 콜백 (`on_quote`) → Kafka `market-quotes` 토픽 발행
    - OrderBook 데이터 수신 콜백 (`on_orderbook`) → Kafka `order-book` 토픽 발행
    - 지수 백오프 재연결 (초기 1초, 최대 30초, 최대 5회)
    - 인증 실패 시 재연결 미시도 + 관리자 알림
    - Kafka 발행 실패 시 로컬 버퍼 저장
    - 한글 주석으로 연결 흐름 및 에러 처리 설명
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_

  - [ ] 4.2 시장 시간 스케줄러 구현 (data_ingester/scheduler.py)
    - APScheduler 기반 개장(09:30 ET)/폐장(16:00 ET) 스케줄링
    - 개장 시 WebSocket 연결 수립, 폐장 시 연결 해제 + 대기 모드
    - 대기 모드 중 CPU 사용률 최소화 (asyncio.sleep 활용)
    - pytz US/Eastern 타임존 처리
    - _Requirements: 9.6, 9.7, 9.8_

  - [ ] 4.3 Data_Ingester 메인 엔트리포인트 (data_ingester/main.py)
    - asyncio 이벤트 루프 설정 및 실행
    - WebSocket 클라이언트 + 스케줄러 통합
    - graceful shutdown 처리 (SIGTERM/SIGINT)
    - _Requirements: 1.1, 9.3_

- [ ] 5. Alert_Engine 구현 (Wave 4)
  - [ ] 5.1 가격 알림 규칙 평가 엔진 구현 (alert_engine/price_evaluator.py)
    - Kafka `market-quotes` 토픽 소비 → 가격 조건 비교
    - 목표가 이상 도달 알림 (PRICE_ABOVE)
    - 하한가 이하 도달 알림 (PRICE_BELOW)
    - 변동률 초과 알림 (PRICE_CHANGE) - 직전 거래일 종가 대비
    - 무효 가격 데이터 (누락/0 이하) 스킵 + 에러 로그
    - 비활성 규칙 (is_active=False) 평가 제외
    - 쿨다운 확인 (5분) 후 중복 알림 억제
    - Redis에서 활성 Alert_Rule 조회 (해시 구조 O(1))
    - 한글 주석으로 각 조건 비교 로직 설명
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

  - [ ] 5.2 호가창 변동 감지 엔진 구현 (alert_engine/orderbook_evaluator.py)
    - Kafka `order-book` 토픽 소비 → 호가 변동 비교
    - 직전 OrderBook 데이터 메모리 캐시 (종목별)
    - 호가 레벨별(1~10호가) 잔량 변동 비율 산출
    - 사용자 설정 임계값(10%~500%) 초과 시 알림 이벤트 생성
    - 수급 불균형 비율 (매수총잔량/매도총잔량) 산출 및 임계값 비교
    - 호가 알림 쿨다운 (60초) 중복 억제
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

  - [ ] 5.3 Alert_Engine 메인 엔트리포인트 (alert_engine/main.py)
    - asyncio 이벤트 루프에서 Quote/OrderBook Consumer 병렬 실행
    - 알림 이벤트 생성 시 Notification_Service로 전달 (Redis pub/sub 또는 직접 호출)
    - Consumer 처리 실패 시 DLQ 전송 (최대 3회 재시도)
    - graceful shutdown 처리
    - _Requirements: 2.6, 4.1, 5.1_

- [ ] 6. Checkpoint - Wave 3~4 검증
  - pytest로 import 에러, 타입 에러 부재 확인
  - Data_Ingester 모듈 인스턴스 생성 가능 여부 확인
  - Alert_Engine 평가 함수 반환 타입 체크
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Notification_Service 구현 (Wave 5)
  - [ ] 7.1 APNs 푸시 알림 클라이언트 구현 (notification_service/apns_client.py)
    - PyAPNs2 기반 HTTP/2 APNs 통신 (토큰 기반 인증)
    - 푸시 페이로드 구성: 종목명, 현재가, 트리거 조건 포함
    - 발송 실패 시 5초 간격 최대 3회 재시도
    - 무효 디바이스 토큰 응답 시 토큰 비활성 처리
    - 1초 이내 전송 요청 완료 목표
    - _Requirements: 6.1, 6.2, 6.3, 6.5, 6.6_

  - [ ] 7.2 이메일 발송 클라이언트 구현 (notification_service/email_client.py)
    - aiosmtplib 기반 비동기 이메일 발송
    - 이메일 본문: 종목명, 현재가, 변동률, 변동 방향, 목표 조건, 알림 시각(KST) 포함
    - 발송 실패 시 5분 후 1회 재시도
    - 최종 실패 시 미전달 상태 Redis 저장
    - 무효 이메일 주소 (None/빈문자열/형식 불일치) 발송 생략 + 에러 로그
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

  - [ ] 7.3 알림 발송 오케스트레이터 구현 (notification_service/orchestrator.py)
    - 알림 채널 결정 로직 (PUSH → EMAIL 폴백, EMAIL 전용)
    - APNs 3회 실패 → 이메일 폴백 전환
    - 발송 완료 시 Redis에 발송 시각 기록
    - 이메일 전용 채널 선택 시 푸시 생략
    - _Requirements: 6.4, 7.1, 7.6_

  - [ ] 7.4 Notification_Service 메인 엔트리포인트 (notification_service/main.py)
    - 알림 이벤트 수신 대기 (Redis pub/sub 또는 Kafka 토픽)
    - 오케스트레이터를 통한 발송 처리
    - graceful shutdown 처리
    - _Requirements: 6.1, 7.1_

- [ ] 8. 사용자 Alert_Rule 관리 API 구현 (Wave 6)
  - [ ] 8.1 Alert_Rule CRUD 모듈 구현 (shared/rule_manager.py)
    - Alert_Rule 생성: 종목 심볼, 알림 유형, 조건값, 채널 저장
    - 사용자당 최대 20개 규칙 제한 (HLEN 체크, 초과 시 거부)
    - Alert_Rule 수정: 1초 이내 Redis 반영
    - Alert_Rule 삭제: Redis 제거 + 쿨다운 타이머 초기화
    - 활성/비활성 상태 토글
    - 입력 유효성 검증: 빈 심볼, 미등록 심볼, 0 이하 조건값 거부
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6_

- [ ] 9. Checkpoint - Wave 5~6 검증
  - pytest로 import 에러, 타입 에러 부재 확인
  - Notification_Service 모듈 인스턴스 생성 가능 여부 확인
  - Rule_Manager CRUD 함수 반환 타입 체크
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 10. 통합 및 문서화 (Wave 7)
  - [ ] 10.1 시장 시간 스케줄링 통합 테스트 설정
    - docker-compose 환경에서 전체 파이프라인 연결 확인
    - 더미 데이터(AAPL, TSLA 2종목)로 Quote → Kafka → Alert_Engine → Notification 흐름 검증
    - 외부 서비스(Alpaca, APNs, SMTP) mock/stub 구성
    - _Requirements: 9.5, 9.6, 9.8_

  - [ ] 10.2 README.md 문서 작성
    - 시스템 개요 및 아키텍처 설명 (한글)
    - Mermaid 다이어그램: 시스템 컴포넌트 구조, 데이터 흐름, Docker 배포 구조
    - 설치 및 실행 방법 (`docker-compose up`)
    - 환경변수 설정 가이드
    - Alert_Rule 설정 예시
    - 각 컴포넌트 역할 및 데이터 흐름 명시
    - _Requirements: 9.5_

- [ ] 11. 속성 기반 테스트 (Property-Based Tests)
  - [ ]* 11.1 Property 1: 데이터 직렬화 Round-Trip 테스트
    - **Property 1: 데이터 직렬화 Round-Trip**
    - Hypothesis로 임의의 유효한 Quote/OrderBook 객체 생성 → JSON 직렬화 → 역직렬화 → 원본 동일성 검증
    - **Validates: Requirements 1.2, 1.3**

  - [ ]* 11.2 Property 2: 로컬 버퍼 크기 불변량 테스트
    - **Property 2: 로컬 버퍼 크기 불변량**
    - 임의의 메시지 시퀀스(0~2000건) 추가 시 버퍼 크기 ≤ 1000 불변량 검증
    - **Validates: Requirements 1.6, 2.4, 2.5**

  - [ ]* 11.3 Property 3: Kafka 파티션 키 일관성 테스트
    - **Property 3: Kafka 파티션 키 일관성**
    - 임의의 symbol + 메시지 조합에서 파티션 키 == symbol 검증
    - **Validates: Requirements 2.2**

  - [ ]* 11.4 Property 4: 가격 알림 조건 동치 테스트
    - **Property 4: 가격 알림 조건 동치**
    - 임의의 (현재가, 목표가/하한가, 임계값) 조합에서 알림 생성 조건 동치 관계 검증
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4**

  - [ ]* 11.5 Property 5: 무효 가격 데이터 알림 미생성 테스트
    - **Property 5: 무효 가격 데이터 알림 미생성**
    - 임의의 무효 가격(0, 음수, None)에서 알림 이벤트 미생성 검증
    - **Validates: Requirements 4.6**

  - [ ]* 11.6 Property 6: 호가 변동 비율 알림 조건 동치 테스트
    - **Property 6: 호가 변동 비율 알림 조건 동치**
    - 임의의 OrderBook 쌍 + 임계값에서 변동 비율 초과 시에만 알림 생성 검증
    - **Validates: Requirements 5.1, 5.2**

  - [ ]* 11.7 Property 7: 수급 불균형 알림 조건 동치 테스트
    - **Property 7: 수급 불균형 알림 조건 동치**
    - 임의의 OrderBook + 임계값에서 불균형 비율 초과 시에만 알림 생성 검증
    - **Validates: Requirements 5.3**

  - [ ]* 11.8 Property 8: 쿨다운 중복 알림 억제 테스트
    - **Property 8: 쿨다운 중복 알림 억제**
    - 임의의 (rule_id, 시간 간격)에서 쿨다운 기간 내 중복 알림 미생성 검증
    - **Validates: Requirements 3.4, 4.5, 5.4**

  - [ ]* 11.9 Property 9: 알림 메시지 필수 필드 포함 테스트
    - **Property 9: 알림 메시지 필수 필드 포함**
    - 임의의 AlertEvent에서 푸시/이메일 페이로드 필수 필드 존재 검증
    - **Validates: Requirements 6.2, 7.2**

  - [ ]* 11.10 Property 10: 무효 이메일 발송 생략 테스트
    - **Property 10: 무효 이메일 발송 생략**
    - 임의의 무효 이메일(None, 빈문자열, 형식 불일치)에서 발송 미시도 검증
    - **Validates: Requirements 7.5**

  - [ ]* 11.11 Property 11: 사용자당 규칙 수 상한 불변량 테스트
    - **Property 11: 사용자당 규칙 수 상한 불변량**
    - 임의의 규칙 등록 시퀀스에서 사용자당 규칙 수 ≤ 20 불변량 검증
    - **Validates: Requirements 8.1**

  - [ ]* 11.12 Property 12: Alert_Rule CRUD Round-Trip 테스트
    - **Property 12: Alert_Rule CRUD Round-Trip**
    - 임의의 유효한 AlertRule 저장 → 조회 동일성, 삭제 → 조회 미존재 검증
    - **Validates: Requirements 8.2, 8.4**

  - [ ]* 11.13 Property 13: 비활성 규칙 평가 제외 테스트
    - **Property 13: 비활성 규칙 평가 제외**
    - 임의의 (Quote, 비활성 Rule) 조합에서 알림 이벤트 미생성 검증
    - **Validates: Requirements 8.5**

  - [ ]* 11.14 Property 14: 무효 입력 등록 거부 테스트
    - **Property 14: 무효 입력 등록 거부**
    - 임의의 무효 심볼/조건값에서 등록 거부 검증
    - **Validates: Requirements 8.6**

- [ ] 12. Final Checkpoint - 전체 시스템 검증
  - docker-compose 환경에서 전체 파이프라인 end-to-end 동작 확인
  - 더미 데이터로 Quote → Kafka → Alert_Engine → Notification 흐름 검증
  - 모든 속성 기반 테스트 통과 확인
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- 모든 코드 파일에 상세한 한글 주석 필수 (함수/클래스/모듈 단위)
- 개별 모듈별 단위 테스트 파일 생성 금지 (testing-guidelines.md 준수)
- wave 단위 마지막 Checkpoint에서만 통합 검증 수행
- 외부 서비스(Alpaca, APNs, SMTP)는 mock/stub으로 대체하여 테스트
- Docker Compose로 `docker-compose up` 단일 명령 배포 가능해야 함
- Redis 메모리 256MB 제한, 컨테이너당 512MB 메모리 제한 준수
- Property-based tests use Hypothesis library with minimum 100 iterations each

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.3", "1.4"] },
    { "id": 1, "tasks": ["2.1", "2.2", "2.3"] },
    { "id": 2, "tasks": ["4.1", "4.2", "4.3"] },
    { "id": 3, "tasks": ["5.1", "5.2", "5.3"] },
    { "id": 4, "tasks": ["7.1", "7.2", "7.3", "7.4"] },
    { "id": 5, "tasks": ["8.1"] },
    { "id": 6, "tasks": ["10.1", "10.2"] },
    { "id": 7, "tasks": ["11.1", "11.2", "11.3", "11.4", "11.5", "11.6", "11.7", "11.8", "11.9", "11.10", "11.11", "11.12", "11.13", "11.14"] }
  ]
}
```
