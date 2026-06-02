# ============================================================
# Commit Log Producer — Docker Image
# Multi-stage build: compile the Java producer, then package
# it into a minimal runtime image.
# ============================================================
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

# Copy project descriptor and source
COPY pom.xml .
COPY src ./src

# Build an executable fat-jar (uses maven-shade-plugin defined in pom.xml)
RUN mvn package -q -DskipTests

# --- Runtime stage ---
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY --from=builder /build/target/commit-log-producer-1.0.jar app.jar

# Default: produce 100 messages; override with --count N via CMD in docker-compose
ENTRYPOINT ["java", "-jar", "app.jar"]
CMD ["--count", "100"]
