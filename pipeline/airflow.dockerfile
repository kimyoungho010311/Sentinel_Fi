FROM apache/airflow:2.7.2-python3.11

# 에어플로우 파이프라인 운영에 필요한 파이썬 라이브러리들 일괄 주입
RUN pip install --no-cache-dir \
    django==5.0.3 \
    psycopg2-binary \
    requests \
    confluent-kafka \
    websockets