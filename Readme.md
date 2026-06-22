# Sentinel-Fi

> Kafka와 Flink 기반 실시간 암호화폐 시세 처리 및 이상 변동 탐지 파이프라인

Sentinel-Fi는 암호화폐 거래소인 업비트(Upbit)에서 발생하는 실시간 거래 데이터를 Kafka로 수집하고, Flink에서 집계와 이상 변동 탐지를 수행한 뒤 PostgreSQL에 저장하는 데이터 파이프라인 프로젝트입니다.

초기에는 금융 데이터 기반 개인화 서비스까지 포함한 웹 서비스로 기획했지만, 데이터 엔지니어 역량을 살려 실시간 데이터 수집, 스트림 처리, 지연 시간 측정, 데이터 품질 검증, 저장 병목 개선을 중심으로 프로젝트의 범위를 재정의했습니다.

---

## 1. 프로젝트 목표

이 프로젝트의 핵심 목표는 실시간 금융 데이터를 안정적으로 수집하고, 처리 과정에서 발생하는 지연과 병목을 정량적으로 관찰할 수 있는 파이프라인을 구성하는 것입니다.

- WebSocket 기반 실시간 시세 데이터를 Kafka로 수집
- Flink로 1초 단위 처리량, 지연 시간, 오류율 집계
- Flink Event Time 윈도우를 사용한 급등락 탐지
- PostgreSQL에 실시간 집계 결과 및 탐지 결과 저장
- 데이터 품질 오류를 분리 저장하여 원인 추적 가능하게 구성
- Django를 통해 API를 구축하여 개발자에게 1초 단위 데이터 흐름정보 제공

---

## 2. 아키텍처

![System Architecture](/img/system_architecture.png)

**여기에 핵심적인 테이블 정보만 넣자.**

---

## 3. 기술 스택

| 영역 | 기술 | 사용 목적 |
| --- | --- | --- |
| Data Source | Upbit WebSocket | 실시간 암호화폐 ticker 데이터 수집 |
| Message Broker | Kafka | 수집 속도와 처리 속도 분리, 메시지 버퍼링 |
| Stream Processing | Apache Flink | 실시간 윈도우 집계, 지연 시간 측정, 급등락 탐지 |
| Database | PostgreSQL | 원천 데이터, 집계 결과, 이상 데이터 저장 |
| Scheduler | Airflow | 마켓 코드 동기화 등 주기 작업 관리 |
| Backend | Django | 데이터 모델링 및 관리 API 기반 |
| Infra | Docker | Kafka, Flink, PostgreSQL, Airflow 로컬 실행 환경 구성 |

---

## 4. 핵심 구현 기능

### 실시간 데이터 수집

Upbit WebSocket을 통해 여러 암호화폐 거래 데이터를 실시간으로 구독합니다. 수신한 데이터는 Kafka `ticker` 토픽으로 전송되며, 이후 Flink와 Consumer가 동일한 토픽을 기준으로 데이터를 처리합니다.

관련 파일:

- `backend-pjt/collector/upbit_ws.py`
- `backend-pjt/collector/kafka_consumer.py`

### Flink 1초 단위 실시간 메트릭 집계

Flink Metric Job은 Kafka에서 ticker 메시지를 읽고 1초 Tumbling Window 단위로 운영 지표를 계산합니다.

집계 항목:

- 초당 수집 데이터 수
- 총 오류 데이터 수
- 오류율
- 평균 지연 시간
- 최대 지연 시간
- 활성 마켓 수
- 초당 총 거래대금

관련 파일:

- `pipeline/jobs/metric_collections.py`
- `backend-pjt/flink_metrics/models.py`

### Flink 슬라이딩 윈도우 기반 급등락 탐지

Drop Detection Job은 마켓별로 데이터를 그룹화한 뒤, 5분 크기의 Sliding Event Time Window에서 시작가와 종가를 비교합니다. 이를 통해 특정 시간 구간의 가격 변화율과 상태를 저장합니다.

> 급등락 기준은 업비트에서 제공되는 기준이 아닌 테스트를 위해 임의로 설정하였습니다.  
> 업비트 급등락 기준: https://support.upbit.com/hc/ko/articles/900005994766-업비트-시장-경보-제도가-무엇인가요

관련 파일:

- `pipeline/jobs/drop_detect.py`
- `pipeline/jobs/utils/metrics.py`
- `pipeline/jobs/utils/watermark.py`
- `backend-pjt/window_stats/models.py`

### Airflow 기반 마켓 코드 동기화

Sentinel-Fi는 Upbit에서 지원하는 마켓 목록을 주기적으로 동기화하기 위해 Airflow DAG를 사용했습니다.

Airflow는 실시간 ticker 처리에는 적합하지 않기 때문에, 초 단위 데이터 흐름과 오류율 계산은 Flink가 담당하고, Airflow는 하루 단위 또는 주기적 관리 작업을 담당하도록 역할을 분리했습니다.

동기화 흐름:

1. Airflow DAG가 Upbit Market API를 호출
2. 현재 지원 중인 마켓 목록을 조회
3. PostgreSQL `market` 테이블의 기존 마켓 목록과 비교
4. 신규 상장 마켓과 비활성화된 마켓을 반영
5. 이후 WebSocket Producer가 최신 마켓 목록을 기준으로 ticker를 구독

관련 파일:

- `pipeline/dags/fetch_coins.py`
- `pipeline/dags/utils.py`
- `backend-pjt/collector/sync_markets.py`
- `backend-pjt/market_data/models.py`

### 데이터 품질 검증

Kafka Consumer는 ticker 데이터를 저장하기 전 필수 필드 누락, 음수 값, 잘못된 범주형 값 등을 검증합니다. 정상 데이터는 `TickerDate`에 저장하고, 비정상 데이터는 원본 데이터와 오류 사유를 함께 `BadTickerDate`에 저장합니다.

품질 검증 테스트 방법은 임의로 비정상 데이터를 만들어 테스트해보았습니다. 실험 방법과 결과는 다음과 같습니다.

먼저 kafka-console-producer.sh에 접속하여 비정상 데이터를 직접 kafka topic에게 전송합니다.

```powershell
docker exec -it sentinel_fi_kafka kafka-console-producer --broker-list localhost:9092 --topic ticker
```

그 다음 거래 금액이 음수인 비정상 데이터를 `JSON`형태로 kafka에게 전송합니다.

```powershell
> {"code": "KRW-BTC", "trade_price": -1, "timestamp": 1234567890, "change": "RISE", "stream_type": "REALTIME"}
```

그 다음 `BadTickerDate` 테이블을 확인하면 아래와 같은 데이터가 생성된 것을 확인할 수 있습니다.

```powershell
id	6
raw_data	{"code": "KRW-BTC", "change": "RISE", "timestamp": 1234567890, "stream_type": "REALTIME", "trade_price": -1}
error_log	Negative Value Error: price or volume is negative
created_at	2026-06-16 06:42:46.831016+00
```

관련 파일:

- `backend-pjt/collector/kafka_consumer.py`
- `backend-pjt/market_data/models.py`

---

## 5. 정량 지표

| 지표 | 설명 | 저장 위치 |
| --- | --- | --- |
| 초당 처리량 | 1초 동안 Flink가 소비한 ticker 메시지 수 | `flink_realtime_metric.total_collected_count` |
| 평균 지연 시간 | 메시지 timestamp와 처리 시각의 평균 차이 | `flink_realtime_metric.average_latency_ms` |
| 최대 지연 시간 | 1초 윈도우 내 가장 큰 지연 시간 | `flink_realtime_metric.max_latency_ms` |
| 오류율 | 전체 메시지 중 비정상 메시지 비율 | `flink_realtime_metric.error_rate_percentage` |
| 활성 마켓 수 | 1초 동안 수신된 고유 마켓 수 | `flink_realtime_metric.unique_market_count` |
| 초당 거래대금 | 1초 동안 처리된 거래대금 합계 | `flink_realtime_metric.total_trade_volume_krw` |
| 윈도우별 변동률 | 5분 윈도우 시작가/종가 기준 변동률 | `window_stats.change_rate` |

---

## 6. 성능 실험 및 개선

### 실험 목적

Flink 기반 실시간 처리 파이프라인에서 처리량과 지연 시간에 영향을 주는 요소를 확인하기 위해 성능 실험을 수행했습니다.

측정 지표는 다음과 같습니다.

- 평균 처리량(events/s)
- 최대 처리량(events/s)
- 평균 지연 시간(ms)
- p95 지연 시간(ms)
- 최대 지연 시간(ms)
- 오류율(%)

### 실험 결과

| 실험 조건 | 샘플 수 | 평균 처리량 | 최대 처리량 | 평균 지연 | p95 지연 | 최대 지연 | 오류율 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 기본 설정 | 360초 | 54.95 events/s | 137 events/s | 1077.15ms | 1713.56ms | 2241.20ms | 0.00% |
| parallelism=2 | 296초 | 65.91 events/s | 255 events/s | 1341.98ms | 2431.94ms | 2431.73ms | 0.00% |
| parallelism=4 | 296초 | 55.40 events/s | 212 events/s | 1056.52ms | 1299.40ms | 2350.82ms | 0.00% |
| print 제거 + DB flush 조정 | 394초 | 746.20 events/s | 1500 events/s | 346.13ms | 623.23ms | 1217.02ms | 0.00% |

### 실험 해석

기본 설정에서는 실제 Upbit ticker 스트림 기준으로 평균 54.95 events/s를 처리했고, 평균 지연 시간은 1077.15ms로 측정되었습니다.

Flink parallelism을 2로 증가시켰을 때 평균 처리량은 증가했지만 평균 지연과 p95 지연도 함께 증가했습니다. parallelism을 4로 증가시켰을 때는 p95 지연은 개선되었지만 평균 처리량은 기본 설정과 큰 차이를 보이지 않았습니다.

이를 통해 단순히 parallelism 값을 높이는 것만으로는 처리 성능이 선형적으로 개선되지 않으며, Kafka 입력량, partition 수, window 집계 방식, DB sink 처리 방식 등 전체 파이프라인 병목을 함께 고려해야 함을 확인했습니다.

이후 synthetic producer를 이용해 고정 부하를 생성하고, 디버깅용 `print()` sink 제거 및 JDBC Sink flush 조건을 조정했습니다. 그 결과 평균 지연 시간과 최대 지연 시간이 감소했으며, 스트리밍 파이프라인에서는 연산 로직뿐 아니라 로그 I/O와 sink flush 조건도 latency에 영향을 줄 수 있음을 확인했습니다.

### 추가 실험: market별 관측성 개선

초기 Metric Job은 `window_all()`을 사용해 전체 스트림을 1초 단위로 집계했습니다. 이 구조는 전체 처리량을 보기에는 단순하지만, 특정 마켓의 지연이나 데이터 유입량 차이를 확인하기 어렵습니다.

이를 보완하기 위해 `key_by(market)` 기반 마켓별 메트릭 집계를 추가했습니다. 해당 구조는 전체 처리 성능을 직접적으로 개선하기보다는, 마켓별 처리량·지연 시간·오류율을 확인할 수 있도록 관측성을 높이는 데 목적이 있습니다. 이 구조는 향후 특정 마켓의 수집 공백 감지 기능으로 확장할 수 있습니다.

### 한계 및 향후 개선

이번 실험은 설정 변경과 SQL 집계를 수동으로 수행했습니다. 이 방식은 반복 실험 시 조건 관리와 결과 기록의 일관성이 떨어질 수 있습니다.

향후에는 benchmark runner를 작성해 실험 조건 설정, 테이블 초기화, synthetic producer 실행, 결과 쿼리, Markdown/CSV 저장을 자동화할 계획입니다. 또한 Prometheus/Grafana를 도입해 Flink backpressure, Kafka consumer lag, PostgreSQL query latency까지 함께 관찰할 수 있도록 확장할 예정입니다.



---

## 7. 데이터 품질 처리

### 정상 데이터와 비정상 데이터 분리 저장

실시간 API 데이터는 외부 시스템에서 전달되기 때문에 필드 누락, 비정상 값, 예상하지 못한 범주형 값이 포함될 수 있습니다. Sentinel-Fi는 저장 전에 다음 조건을 검증합니다.

- `code`, `trade_price`, `timestamp` 필수 필드 존재 여부
- `trade_price`, `trade_volume` 음수 여부
- `change` 값이 `RISE`, `EVEN`, `FALL` 중 하나인지 확인
- `stream_type` 값이 `SNAPSHOT`, `REALTIME` 중 하나인지 확인

정상 데이터는 `TickerDate`에 저장하고, 비정상 데이터는 `BadTickerDate`에 저장합니다. 이때 원본 raw data와 오류 사유를 함께 남겨 이후 데이터 품질 문제를 추적할 수 있도록 했습니다. 아래는 저장되는 데이터 예시입니다.

```json
{
  "raw_data": {
    "code": "KRW-BTC",
    "change": "RISE",
    "timestamp": 1779096039644,
    "stream_type": "REALTIME",
    "trade_price": -1000,
    "trade_volume": 0.5
  },
  "error_log": {
		"Negative Value Error: price or volume is negative"
  },
  "created_at": "2026-05-22 00:35:19.90805+00"
}
```

---

## 8. 트러블슈팅

### PyFlink 커스텀 모듈 수정 사항 미반영 및 Import Error
#### 문제
Flink Job 스크립트(drop_detect.py)를 수정하고 컨테이너를 재시작해도 변경 사항이 반영되지 않거나, 내부에서 참조하는 커스텀 모듈(utils.watermark)을 찾을 수 없다는 에러가 발생했습니다.

#### 원인 분석
원인은 Flink의 독특한 Python 부모-자식 프로세스(Beam Worker) 구조와 Docker 빌드 시점의 한계 때문이었습니다.

Flink는 내부적으로 Python 코드를 실행하기 위해 Apache Beam Worker를 실행합니다.

메인 스크립트는 마운트된 경로(@/opt/flink/pipeline)에서 실행되지만, 서브 모듈을 import할 때 Beam Worker는 이 경로를 알지 못하고 Python의 기본 라이브러리 경로인 site-packages만 탐색합니다.

기존 Dockerfile에서는 빌드 시점에 COPY jobs/utils/ /usr/local/lib/.../site-packages/utils/ 명령을 통해 코드를 딱 한 번 복사했기 때문에, 로컬에서 코드를 수정해도 컨테이너 내부의 site-packages에는 옛날 코드가 그대로 남아있어 수정본이 반영되지 않았습니다.

#### 해결 방법
로컬의 커스텀 모듈 폴더를 Docker 컨테이너 내부 Beam Worker가 참조하는 site-packages 경로에 직접 볼륨 마운트(Volume Mount)하여, 로컬의 수정 사항이 컨테이너 내부에 실시간으로 동기화되도록 해결했습니다.

```YAML
# docker-compose.yml
services:
  flink-jobmanager:
    volumes:
      - ./pipeline/jobs:/opt/flink/pipeline
      - ./pipeline/jobs/utils:/usr/local/lib/python3.10/dist-packages/utils # site-packages 직통 마운트
```
```Plaintext
[실행 흐름]
Beam Worker 실행 ➔ from utils.watermark import ... ➔ site-packages 탐색 ➔ 마운트된 로컬 utils 코드 즉시 참조
```

### Flink TaskManager 미등록으로 인한 Job 제출 실패 (NoResourceAvailableException)
#### 문제
환경 마이그레이션(OS 변경) 후 Flink에 파이썬 데이터 파이프라인 파일을 제출(Submit)했을 때, 상태가 RUNNING으로 넘어가지 않고 즉시 잡이 취소되며 에러가 발생했습니다. Flink 대시보드에서 Available Task Slots: 0으로 표시되었습니다.

```Bash
py4j.protocol.Py4JJavaError: An error occurred while calling o0.execute.
Caused by: org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException: Could not acquire the minimum required resources.
```
#### 원인 분석
실제 연산을 담당하는 TaskManager 컨테이너의 로그를 추적한 결과, 다음과 같은 네트워크 연결 거부 에러를 발견했습니다.

```Bash
Association with remote system [pekko.tcp://flink@<이전_컨테이너_ID>:6123] has failed ... Connection refused
```
TaskManager가 JobManager를 찾아가서 자격(Task Slot)을 등록해야 하는데, 환경이 바뀌면서 JobManager를 가리키는 네트워크 주소 설정(JOB_MANAGER_RPC_ADDRESS)이 누락되어 이전 환경의 고정된 컨테이너 ID를 들고 찾아가다 접속에 실패한 것이 원인이었습니다.

#### 해결 방법
docker-compose.yml 환경 변수에 Docker 내장 DNS가 컨테이너 이름을 기반으로 서로를 찾을 수 있도록 원격 프로시저 호출(RPC) 주소를 명시적으로 지정해 해결했습니다.

```YAML
# docker-compose.yml
services:
  flink-taskmanager:
    environment:
      - JOB_MANAGER_RPC_ADDRESS=flink-jobmanager # JobManager 서비스명 명시
```
### Airflow 내 내장 모듈 및 타 프레임워크(Django) 환경 참조 실패
#### 문제
Airflow DAG를 통해 업비트 마켓 코드를 주기적으로 동기화하는 태스크를 실행할 때, 프로젝트 공통 모듈 및 Django 환경을 로드하지 못하는 문제가 발생했습니다.

``` Bash
ModuleNotFoundError: No module named 'django'
ModuleNotFoundError: No module named 'config'
```
#### 원인 분석
Airflow 스케줄러와 워커 컨테이너가 로컬 파일 시스템에 있는 Django 비즈니스 로직 폴더(backend-pjt)의 위치를 알지 못했고, Airflow 컨테이너 자체에 django 패키지가 설치되어 있지 않아 발생한 격리 환경 문제였습니다.

#### 해결 방법
Airflow 컨테이너 빌드 시점에 필요한 패키지들이 설치되도록 커스텀 Dockerfile 설정을 보완하고, Django 프로젝트 루트 폴더를 Airflow 컨테이너 내부로 통째로 볼륨 맵핑하여 Airflow 작업 내에서 Django ORM 및 공통 설정을 그대로 임포트해 사용할 수 있도록 구조를 맞췄습니다.

```YAML
# docker-compose.yml
services:
  airflow-worker:
    build:
      context: ./pipeline
      dockerfile: airflow.dockerfile # 내부에서 pip install django 수행
    volumes:
      - ./pipeline/dags:/opt/airflow/dags
      - ./backend-pjt:/opt/airflow/backend-pjt # Django 루트 폴더 맵핑
```
---

## 9. 실행 방법

### 인프라 실행

```bash
docker compose up -d
```

### Django 마이그레이션

```bash
cd backend-pjt
python manage.py migrate
```

### Upbit WebSocket Producer 실행

```bash
python backend-pjt/collector/upbit_ws.py
# 터미널 분할 후
python backend-pjt/collector/kafka-consumer.py
```

### Flink Job 실행

```bash
docker exec -it sentinel_fi_flink_jobmanager flink run -py /opt/flink/pipeline/drop_detect.py
# 터미널 분할 후
docker exec -it sentinel_fi_flink_jobmanager flink run -py /opt/flink/pipeline/metric_collections.py
```