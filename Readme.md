# 📈 금융 데이터 기반 실시간 이상 징후 탐지 & 맞춤형 자산 큐레이션 서비스

> 작성일: 2026-05-08
버전: v1.0 (초안)
> 

---

## 목차

1. [서비스 개요 & 목표](https://claude.ai/chat/71fa7d37-557a-439b-9c9f-1d3268146712#1-%EC%84%9C%EB%B9%84%EC%8A%A4-%EA%B0%9C%EC%9A%94--%EB%AA%A9%ED%91%9C)
2. [기술 스택 선택 근거](https://claude.ai/chat/71fa7d37-557a-439b-9c9f-1d3268146712#2-%EA%B8%B0%EC%88%A0-%EC%8A%A4%ED%83%9D-%EC%84%A0%ED%83%9D-%EA%B7%BC%EA%B1%B0)
3. [데이터 파이프라인 설계](https://claude.ai/chat/71fa7d37-557a-439b-9c9f-1d3268146712#3-%EB%8D%B0%EC%9D%B4%ED%84%B0-%ED%8C%8C%EC%9D%B4%ED%94%84%EB%9D%BC%EC%9D%B8-%EC%84%A4%EA%B3%84)
4. [DB 스키마 설계](https://claude.ai/chat/71fa7d37-557a-439b-9c9f-1d3268146712#4-db-%EC%8A%A4%ED%82%A4%EB%A7%88-%EC%84%A4%EA%B3%84)
5. [API 명세 초안](https://claude.ai/chat/71fa7d37-557a-439b-9c9f-1d3268146712#5-api-%EB%AA%85%EC%84%B8-%EC%B4%88%EC%95%88)
6. [개발 일정 & 역할 분담](https://claude.ai/chat/71fa7d37-557a-439b-9c9f-1d3268146712#6-%EA%B0%9C%EB%B0%9C-%EC%9D%BC%EC%A0%95--%EC%97%AD%ED%95%A0-%EB%B6%84%EB%8B%B4)

---

## 1. 서비스 개요 & 목표

### 한 줄 소개

> 주식·암호화폐 데이터를 실시간으로 수집·분석하여, 이상 징후를 탐지하고 사용자 행동 기반 맞춤형 자산 정보를 큐레이션하는 웹 서비스
> 

### 핵심 가치

| 구분 | 내용 |
| --- | --- |
| 기존 서비스와의 차이 | 단순 시세 조회가 아닌, **통계 기반 이상 탐지 + 개인화 대시보드** 제공 |
| 대상 사용자 | 금융 데이터에 관심 있는 개인 투자자 |
| 핵심 차별점 | AI 없이 엔지니어링 로직(DTW, 상관관계, 표준편차)만으로 인사이트 생성 |

### 주요 기능 목록

**기본 기능**

- 실시간 주식·암호화폐 시세 목록 및 상세 조회
- 종목 좋아요(관심 등록) 및 활동 로그 기록
- 회원가입 / 로그인 / 로그아웃 (JWT 인증)
- 종목·뉴스 검색 (Elasticsearch)

**인사이트 기능**

- 급락·급등 이상 징후 실시간 탐지 및 경보 (Flink CEP)
- 현재 차트와 과거 유사 패턴 매칭 (Spark DTW)
- 상관관계 높은 두 종목 간 괴리율 실시간 포착
- 표준편차(σ)를 벗어나는 이상 변동 자동 알림

**개인화 기능**

- 사용자별 맞춤 자산 큐레이션 (조회수·좋아요 기반)
- 활동 대시보드: 관심 종목 변동률, 카테고리별 조회 통계
- 최근 본 종목 목록 / 좋아요 목록 시각화

---

## 2. 기술 스택 선택 근거

### 전체 구성

```
[외부 API] → Kafka → Flink → PostgreSQL / Redis
                   → HDFS  → Spark → Airflow
                                    ↓
                             Django DRF → Vue.js
                             Elasticsearch
```

![image.png](img\image.png)

### 레이어별 선택 근거

### 수집 레이어

| 기술 | 선택 이유 | 대안 대비 장점 |
| --- | --- | --- |
| **외부 금융 API** (Upbit, Binance, KRX) | WebSocket push 방식으로 초당 수십~수백 건 수집 | 폴링 방식 대비 지연 최소화 |
| **Kafka** | 초당 수만 건 메시지를 유실 없이 완충하는 메시지 브로커 | Flink·Spark 둘 다 Kafka를 소스로 쓸 수 있어 단일 수집 창구 역할 |

> Kafka 없이 Flink가 API를 직접 구독하면, 처리 속도가 소비 속도를 못 따라갈 때 데이터 유실 발생
> 

### 실시간 처리 레이어

| 기술 | 선택 이유 | 구체적 역할 |
| --- | --- | --- |
| **Flink** | 스트리밍 처리 특화, 밀리초 단위 연산 가능 | CEP로 급락·급등 탐지, 이동평균·표준편차 계산, PostgreSQL 적재 |
| **Redis** | "자주 바뀌고 자주 읽히는" 실시간 시세를 메모리에 캐싱 | PostgreSQL 부하 분산 (응답 속도 100~1000배 향상) |

> Redis 캐시 패턴: Flink가 시세를 PostgreSQL에 쓸 때 동시에 Redis에도 갱신 → Django는 Redis에서 1ms 이내 응답, cache miss 시에만 PostgreSQL 조회
> 

### 배치 처리 레이어

| 기술 | 선택 이유 | 구체적 역할 |
| --- | --- | --- |
| **HDFS** | 수년치 Raw 데이터 장기 보존 (데이터 레이크) | PostgreSQL은 서비스용, HDFS는 분석용 원본 저장소 |
| **Spark** | TB급 과거 데이터 병렬 분산 처리 | DTW 패턴 매칭, 종목 간 상관관계 매트릭스, 사용자 행동 분석 → Insight 테이블 적재 |
| **Airflow** | 배치 작업 스케줄링 및 의존 관계 관리 | 매일 새벽 2시 상관관계 계산 → 완료 시 추천 점수 업데이트 DAG |

### 서비스 레이어

| 기술 | 선택 이유 | 구체적 역할 |
| --- | --- | --- |
| **PostgreSQL** | 관계형 데이터 관리, Django ORM 궁합 우수 | User·Asset·MarketData·UserLog·Insight 5개 핵심 테이블 |
| **Elasticsearch** | PostgreSQL LIKE 검색 대비 역색인 구조로 압도적 속도 | 종목명 초성 검색, 뉴스 전문 검색 |
| **Django REST Framework** | Python 생태계 (Pandas·NumPy 연동), Serializer-View-URL 구조 | REST API 서버, JWT 인증 |
| **Vue.js** | SPA로 페이지 전환 없이 실시간 시세 업데이트 가능 | Vue Router 라우팅, Chart.js/ECharts 대시보드 |

---

## 3. 데이터 파이프라인 설계

### 실시간 파이프라인 (Kafka → Flink → PostgreSQL/Redis)

```
[Upbit WebSocket]  ─┐
[Binance WebSocket] ─┤→ Kafka Producer → [Topic: market-raw]
[KRX REST Polling] ─┘

[Topic: market-raw]
  → Flink Consumer
      ├─ 이동평균 / 표준편차 계산
      ├─ CEP: 5% 이상 급변 시 Alert 이벤트 발행 → [Topic: alerts]
      ├─ PostgreSQL: MarketData INSERT
      └─ Redis: SET price:{asset_id} (TTL 2초)
```

### 배치 파이프라인 (Kafka → HDFS → Spark → Airflow)

```
Kafka → Kafka Connect (HDFS Sink) → HDFS /raw/market/{date}/

Airflow DAG (매일 02:00)
  Task 1: Spark 상관관계 매트릭스 계산
    → HDFS 과거 데이터 로드
    → 종목 간 Pearson r 연산
    → PostgreSQL Insight 테이블 UPDATE

  Task 2: Spark DTW 패턴 매칭
    → 현재 30일 차트 vs 과거 데이터 유사도 계산
    → 상위 3개 유사 구간 추출
    → PostgreSQL Insight 테이블 UPDATE

  Task 3: 사용자 행동 분석
    → UserLog 집계 (카테고리별 조회수, 좋아요 빈도)
    → 맞춤 추천 점수 계산
    → PostgreSQL Insight 테이블 UPDATE
```

### Elasticsearch 마이그레이션

```
PostgreSQL [Asset 테이블]
  → Django Management Command (초기 1회)
  → Elasticsearch Index: assets
      mapping:
        name: text (analyzer: korean)
        ticker: keyword
        category: keyword
        description: text

PostgreSQL [뉴스/인사이트]
  → Logstash 또는 커스텀 스크립트 (주기적 동기화)
  → Elasticsearch Index: news
```

---

## 4. DB 스키마 설계

### ERD 개요

```
User ──< UserLog >── Asset
 |                    |
 └── Portfolio        └── MarketData
                      |
                      └── Insight
```

### 테이블 상세

### User (사용자)

| 컬럼 | 타입 | 설명 |
| --- | --- | --- |
| id | UUID PK | 고유 식별자 |
| username | VARCHAR(50) UNIQUE | 사용자명 |
| email | VARCHAR(255) UNIQUE | 이메일 |
| password | VARCHAR(255) | bcrypt 해싱 |
| preferred_categories | JSONB | 관심 카테고리 (ex: ["crypto", "stock"]) |
| created_at | TIMESTAMP | 가입일 |
| last_login | TIMESTAMP | 최근 로그인 |

### Asset (종목)

| 컬럼 | 타입 | 설명 |
| --- | --- | --- |
| id | UUID PK | 고유 식별자 |
| ticker | VARCHAR(20) UNIQUE | 종목 코드 (ex: BTC, AAPL) |
| name | VARCHAR(100) | 종목명 |
| category | VARCHAR(20) | 구분 (crypto / stock / etf) |
| market | VARCHAR(20) | 거래소 (Upbit, NYSE, KRX 등) |
| description | TEXT | 종목 설명 |
| created_at | TIMESTAMP | 등록일 |

### MarketData (시세)

| 컬럼 | 타입 | 설명 |
| --- | --- | --- |
| id | BIGSERIAL PK | 고유 식별자 |
| asset_id | UUID FK → Asset | 종목 참조 |
| price | NUMERIC(20, 8) | 현재가 |
| change_rate | NUMERIC(8, 4) | 등락률 (%) |
| volume | NUMERIC(20, 4) | 거래량 |
| is_anomaly | BOOLEAN | 이상 징후 여부 (Flink 탐지) |
| anomaly_type | VARCHAR(20) | 이상 유형 (flash_crash / spike 등) |
| timestamp | TIMESTAMP | 시세 기준 시각 |

> 파티셔닝 고려: timestamp 기준 월별 파티션 (데이터 증가 대비)
> 

### UserLog (사용자 활동)

| 컬럼 | 타입 | 설명 |
| --- | --- | --- |
| id | BIGSERIAL PK | 고유 식별자 |
| user_id | UUID FK → User | 사용자 참조 |
| asset_id | UUID FK → Asset | 종목 참조 |
| action | VARCHAR(20) | 행동 유형 (view / like / unlike / click) |
| created_at | TIMESTAMP | 행동 발생 시각 |

### Insight (분석 결과)

| 컬럼 | 타입 | 설명 |
| --- | --- | --- |
| id | UUID PK | 고유 식별자 |
| asset_id | UUID FK → Asset | 종목 참조 |
| insight_type | VARCHAR(30) | 인사이트 유형 (pattern_match / correlation / anomaly_summary) |
| data | JSONB | Spark 분석 결과 (유연한 구조) |
| generated_at | TIMESTAMP | 분석 생성 시각 |

```json
// data 예시 (pattern_match)
{
  "similar_periods": [
    { "start": "2021-03-01", "end": "2021-04-15", "similarity": 0.94 },
    { "start": "2022-11-10", "end": "2022-12-20", "similarity": 0.89 }
  ],
  "next_30d_avg_return": -3.2
}

// data 예시 (correlation)
{
  "correlated_assets": [
    { "ticker": "ETH", "r": 0.91 },
    { "ticker": "BNB", "r": 0.78 }
  ],
  "divergence_alert": true,
  "divergence_pct": 4.7
}
```

### 인덱스 전략

```sql
-- 자주 조회되는 패턴 최적화
CREATE INDEX idx_marketdata_asset_timestamp ON market_data(asset_id, timestamp DESC);
CREATE INDEX idx_userlog_user_action ON user_log(user_id, action, created_at DESC);
CREATE INDEX idx_insight_asset_type ON insight(asset_id, insight_type);
```

---

## 5. API 명세 초안

### Base URL

```
http://localhost:8000/api/v1/
```

### 인증

JWT Bearer Token 방식

```
Authorization: Bearer <access_token>
```

---

### 인증 관련

### POST /auth/register/ — 회원가입

**Request Body**

```json
{
  "username": "john_doe",
  "email": "john@example.com",
  "password": "securePass123!"
}
```

**Response 201**

```json
{
  "id": "uuid",
  "username": "john_doe",
  "email": "john@example.com"
}
```

---

### POST /auth/login/ — 로그인

**Request Body**

```json
{
  "email": "john@example.com",
  "password": "securePass123!"
}
```

**Response 200**

```json
{
  "access": "eyJ...",
  "refresh": "eyJ..."
}
```

---

### POST /auth/logout/ — 로그아웃 `🔒 인증 필요`

**Request Body**

```json
{ "refresh": "eyJ..." }
```

**Response 205** — No Content (Redis에 refresh 토큰 블랙리스트 등록)

---

### 자산(종목) 관련

### GET /contents-list/ — 자산 목록

**Query Parameters**

| 파라미터 | 타입 | 설명 |
| --- | --- | --- |
| category | string | crypto / stock / etf |
| sort | string | price_change / volume / likes |
| search | string | 종목명 또는 ticker 검색 |
| page | int | 페이지 번호 (기본 1) |
| page_size | int | 페이지당 개수 (기본 20) |

**Response 200**

```json
{
  "count": 100,
  "next": "/api/v1/contents-list/?page=2",
  "results": [
    {
      "id": "uuid",
      "ticker": "BTC",
      "name": "Bitcoin",
      "category": "crypto",
      "price": 95000000,
      "change_rate": 2.34,
      "is_anomaly": false,
      "like_count": 1204
    }
  ]
}
```

---

### GET /contents/{id}/ — 자산 상세

**Response 200**

```json
{
  "id": "uuid",
  "ticker": "BTC",
  "name": "Bitcoin",
  "category": "crypto",
  "market": "Upbit",
  "description": "...",
  "latest_price": {
    "price": 95000000,
    "change_rate": 2.34,
    "volume": 3200.55,
    "timestamp": "2026-05-08T12:00:00Z"
  },
  "insight": {
    "pattern_match": { ... },
    "correlation": { ... }
  },
  "is_liked": true
}
```

---

### POST /like/ — 좋아요 토글 `🔒 인증 필요`

**Request Body**

```json
{ "asset_id": "uuid" }
```

**Response 200**

```json
{
  "asset_id": "uuid",
  "liked": true,
  "like_count": 1205
}
```

---

### 대시보드 관련

### GET /dashboard/ — 사용자 활동 통계 `🔒 인증 필요`

**Response 200**

```json
{
  "liked_assets": [
    { "ticker": "BTC", "name": "Bitcoin", "change_rate": 2.34 }
  ],
  "recent_viewed": [
    { "ticker": "ETH", "name": "Ethereum", "viewed_at": "2026-05-08T11:30:00Z" }
  ],
  "category_stats": {
    "crypto": 45,
    "stock": 30,
    "etf": 5
  },
  "activity_by_date": [
    { "date": "2026-05-01", "views": 12, "likes": 3 }
  ]
}
```

---

### GET /dashboard/alerts/ — 이상 징후 알림 목록 `🔒 인증 필요`

**Response 200**

```json
{
  "alerts": [
    {
      "asset": { "ticker": "BTC", "name": "Bitcoin" },
      "anomaly_type": "flash_crash",
      "change_rate": -8.3,
      "detected_at": "2026-05-08T09:15:00Z"
    }
  ]
}
```

---

### 검색

### GET /search/ — 통합 검색 (Elasticsearch)

**Query Parameters**

| 파라미터 | 타입 | 설명 |
| --- | --- | --- |
| q | string | 검색어 (종목명, ticker, 뉴스 키워드) |
| type | string | assets / news / all (기본: all) |

**Response 200**

```json
{
  "assets": [
    { "id": "uuid", "ticker": "BTC", "name": "Bitcoin", "score": 0.98 }
  ],
  "news": [
    { "title": "비트코인 급등 배경 분석", "published_at": "2026-05-08T06:00:00Z" }
  ]
}
```

---

### 에러 응답 형식

```json
{
  "error": {
    "code": "UNAUTHORIZED",
    "message": "인증이 필요합니다.",
    "detail": "JWT token이 만료되었습니다."
  }
}
```

| HTTP 코드 | 코드 | 상황 |
| --- | --- | --- |
| 400 | VALIDATION_ERROR | 요청 파라미터 오류 |
| 401 | UNAUTHORIZED | 인증 토큰 없음/만료 |
| 403 | FORBIDDEN | 권한 없음 |
| 404 | NOT_FOUND | 리소스 없음 |
| 429 | RATE_LIMITED | 요청 한도 초과 |
| 500 | SERVER_ERROR | 서버 내부 오류 |

---

## 6. 개발 일정 & 역할 분담

### 전체 일정 (예시: 5주)

| 주차 | 목표 | 산출물 |
| --- | --- | --- |
| **1주차** | 환경 설정 + 설계 확정 | DB 스키마 확정, API 명세서, Docker Compose 환경 |
| **2주차** | 백엔드 기초 + 데이터 수집 | Django 기본 API, Kafka Producer, Flink 기초 연산 |
| **3주차** | 프론트엔드 + 인증 | Vue Router 구조, 로그인/회원가입 UI, JWT 연동 |
| **4주차** | 대시보드 + 인사이트 | Chart.js 대시보드, Spark 배치 로직, Elasticsearch 검색 |
| **5주차** | 통합 + 마무리 | 전체 통합 테스트, API 명세서 완성, 발표 준비 |

> 공통: Docker Compose 환경 구성, API 명세서 작성, 코드 리뷰
> 

---

### 산출물 디렉터리 구조

```
de-pjt/
├── backend-pjt/        # Django REST Framework
│   ├── apps/
│   │   ├── auth/       # 회원관리, JWT
│   │   ├── assets/     # 종목 목록·상세·좋아요
│   │   ├── dashboard/  # 대시보드·알림
│   │   └── search/     # Elasticsearch 연동
│   └── config/
├── front-pjt/          # Vue.js
│   └── src/
│       ├── views/
│       │   ├── LoginView.vue
│       │   ├── MainView.vue
│       │   ├── ContentsView.vue
│       │   ├── DetailView.vue
│       │   └── DashboardView.vue
│       └── components/
├── data-pjt/           # 데이터 파이프라인
│   ├── kafka/          # Producer 설정
│   ├── flink/          # CEP·집계 로직
│   ├── spark/          # 배치 분석 스크립트
│   └── airflow/        # DAG 파일
└── docs/
    └── api-spec.md     # API 명세서
```

---