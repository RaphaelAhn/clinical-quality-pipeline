FROM eclipse-temurin:8-jre
ARG EMBULK_VERSION=0.11.5
ADD https://github.com/embulk/embulk/releases/download/v${EMBULK_VERSION}/embulk-${EMBULK_VERSION}.jar /opt/embulk.jar
RUN useradd --create-home --uid 10001 runner && chmod 644 /opt/embulk.jar
COPY *.yml /opt/configs/
USER runner
WORKDIR /data
# user.home comes from passwd, not HOME; a --user uid without a passwd entry breaks Embulk.
ENTRYPOINT ["java", "-Duser.home=/tmp", "-jar", "/opt/embulk.jar", "run"]
