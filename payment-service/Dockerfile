# ── Build stage ───────────────────────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-22-alpine AS builder

WORKDIR /build

# Copy parent pom first for dependency resolution
COPY pom.xml .

# Copy ALL module poms (Maven needs all of them to resolve the parent)
COPY shared-events/pom.xml shared-events/pom.xml
COPY order-service/pom.xml order-service/pom.xml
COPY payment-service/pom.xml payment-service/pom.xml
COPY notification-service/pom.xml notification-service/pom.xml

# Copy source of only the modules we need to build
COPY shared-events/src shared-events/src
COPY payment-service/src payment-service/src

# Build only payment-service and its dependency (shared-events)
RUN mvn clean package -pl shared-events,payment-service -am -DskipTests

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM eclipse-temurin:22-jre-alpine

WORKDIR /app

# Security: run as non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

COPY --from=builder /build/payment-service/target/payment-service-*.jar app.jar

EXPOSE 8082

ENTRYPOINT ["java", \
  "-XX:+UseContainerSupport", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-jar", "app.jar"]