FROM eclipse-temurin:21-jre-jammy
RUN groupadd -r kneo && useradd -r -g kneo kneo

WORKDIR /app
COPY target/datanest-1.0.0-SNAPSHOT-runner.jar app.jar
RUN chown kneo:kneo app.jar
USER kneo
EXPOSE 8080 38799
ENTRYPOINT ["java", "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", "-jar", "app.jar"]
