FROM maven:3.8-openjdk-11 AS builder

WORKDIR /app

# Copy POM
COPY pom.xml .

# Download dependencies
RUN mvn dependency:go-offline

# Copy source
COPY src ./src

# Build
RUN mvn clean package

#=========
# Runtime image

FROM flink:1.18-java11

WORKDIR /app

# Copy JAR from builder
COPY --from=builder /app/target/flink-engagement-processor-1.0.jar /app/

# Set environment
ENV FLINK_HOME=/opt/flink

CMD ["java", "-cp", "/app/flink-engagement-processor-1.0.jar:/opt/flink/lib/*", "com.streaming.EngagementProcessor"]