FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app

# 실습 가이드 버전 유지
RUN pip install --no-cache-dir \
    kafka-python==2.0.4 \
    confluent-kafka==2.3.0 \
    requests  # API 호출을 위해 추가 권장

COPY . /app/

CMD ["python3"]