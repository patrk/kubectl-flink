# The demo jobs on the image the operator runs. Built and loaded into kind by make demo-image.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -q -B dependency:go-offline
COPY src src
RUN --mount=type=cache,target=/root/.m2 mvn -q -B -o package

FROM flink:2.2
COPY --from=build /src/target/demo-job.jar /opt/flink/usrlib/demo-job.jar
