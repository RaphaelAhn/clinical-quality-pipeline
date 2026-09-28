# Digdag + Embulk + pipeline in one image (build context: project root).
# Python 3.12 base (duckdb/GX wheels) with the Temurin 8 JRE copied in for Digdag and Embulk.
FROM eclipse-temurin:8-jre AS jre
FROM python:3.12-slim
ARG DIGDAG_VERSION=0.10.5
ARG EMBULK_VERSION=0.11.5
COPY --from=jre /opt/java/openjdk /opt/java
ENV JAVA_HOME=/opt/java PATH=/opt/java/bin:$PATH PYTHONPATH=/opt/lib GX_ANALYTICS_ENABLED=false TQDM_DISABLE=1
RUN pip install --no-cache-dir duckdb==1.4.4 great_expectations==1.23.2
ADD https://dl.digdag.io/digdag-${DIGDAG_VERSION}.jar /opt/digdag.jar
ADD https://github.com/embulk/embulk/releases/download/v${EMBULK_VERSION}/embulk-${EMBULK_VERSION}.jar /opt/embulk.jar
COPY embulk/*.yml /opt/configs/
COPY clinical_pipeline.py gx_check.py /opt/lib/
COPY digdag/clinical.dig digdag/verify.sh /opt/workflow/
RUN useradd --create-home --uid 10001 runner && chmod 644 /opt/*.jar && mkdir /data && chown runner /data /opt/workflow
USER runner
WORKDIR /opt/workflow
