FROM eclipse-temurin:8-jre
ARG EMBULK_VERSION=0.11.5
ADD https://github.com/embulk/embulk/releases/download/v${EMBULK_VERSION}/embulk-${EMBULK_VERSION}.jar /opt/embulk.jar
RUN useradd --create-home --uid 10001 runner && chmod 644 /opt/embulk.jar
COPY *.yml /opt/configs/
USER runner
WORKDIR /data
ENTRYPOINT ["java", "-jar", "/opt/embulk.jar", "run"]
