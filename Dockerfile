FROM eclipse-temurin:17-jre
WORKDIR /app
ARG OCI_REVISION
ARG OCI_SOURCE
ARG JAR_FILE=target/rippleguard-governance-service-0.0.1-SNAPSHOT.jar
LABEL org.opencontainers.image.revision="${OCI_REVISION}" \
      org.opencontainers.image.source="${OCI_SOURCE}"
RUN test -n "${OCI_REVISION}" \
    && test "${OCI_REVISION}" != "unknown" \
    && test -n "${OCI_SOURCE}" \
    && test "${OCI_SOURCE}" != "unknown"
COPY ${JAR_FILE} app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
