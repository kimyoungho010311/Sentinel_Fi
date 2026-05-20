# 1. 베이스 이미지
FROM flink:1.19-scala_2.12-java17

# 2. 루트 권한 획득
USER root

# 3. [수정] 시스템 JDK를 명시적으로 설치하여 include 경로 문제를 해결합니다.
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

# 4. [핵심] 새로 설치된 시스템 JDK 경로로 JAVA_HOME 설정 (Ubuntu 표준 경로)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# 5. pip 업그레이드
RUN pip3 install --upgrade pip

# 6. PyFlink 및 라이브러리 설치
# 이제 /usr/lib/jvm/... 경로에 include 폴더가 확실히 존재하므로 에러가 나지 않습니다.
RUN pip3 install --no-cache-dir \
    apache-flink==1.19.0 \
    pandas \
    --break-system-packages

# 7. 환경 정리
RUN ln -s /usr/bin/python3 /usr/bin/python && \
    cp /opt/flink/opt/flink-python-*.jar /opt/flink/lib/

# 8. 커넥터 복사
COPY flink-sql-connector-kafka-3.3.0-1.19.jar /opt/flink/lib/
# 9. 파이프라인 코드 복사
COPY jobs/utils/ /usr/local/lib/python3.10/dist-packages/utils/

WORKDIR /opt/flink