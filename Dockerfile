# syntax=docker/dockerfile:1

# One Dockerfile for all six services. The build stage is identical for every one of them,
# so BuildKit builds the reactor once and reuses that layer for all six images; only the
# final COPY differs.

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Poms first, so a source-only change does not re-resolve every dependency.
COPY pom.xml .
COPY order-service/pom.xml order-service/
COPY inventory-service/pom.xml inventory-service/
COPY payment-service/pom.xml payment-service/
COPY notification-service/pom.xml notification-service/
COPY saga-orchestrator/pom.xml saga-orchestrator/
COPY scheduler-service/pom.xml scheduler-service/
COPY integration-tests/pom.xml integration-tests/
RUN mvn -B -q dependency:go-offline -DskipTests || true

COPY . .

# Tests are skipped here on purpose. They need Docker themselves (Testcontainers), and
# running them inside the image build would mean docker-in-docker for no benefit — `mvn
# test` on the host is where they belong, and CI runs them there.
RUN mvn -B -q package -DskipTests


FROM eclipse-temurin:21-jre AS runtime

# curl is here only so Compose can run a real HTTP health check against the services that
# expose one. Without it the healthcheck would have nothing to call and `depends_on:
# condition: service_healthy` would be a lie.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

ARG SERVICE
ENV SERVICE=${SERVICE}
WORKDIR /app

# The -exec classifier, not the plain jar. `spring-boot-maven-plugin` attaches the runnable
# jar under that classifier so the plain one stays usable as a library by
# integration-tests — see ARCHITECTURE.md section 8.4. Copying the wrong one gives a jar
# with no Main-Class.
COPY --from=build /build/${SERVICE}/target/${SERVICE}-0.0.1-SNAPSHOT-exec.jar /app/app.jar

# Not run as root. Nothing here needs it.
RUN useradd --system --uid 10001 saga && chown -R saga:saga /app
USER saga

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
