FROM eclipse-temurin:21-jre-jammy
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*
RUN groupadd -r kneo && useradd -r -g kneo kneo

RUN mkdir -p /app/controller-uploads /app/file-uploads /var/log/datanest \
    && chown -R kneo:kneo /app /var/log/datanest
WORKDIR /app
COPY target/datanest-*-runner.jar app.jar
RUN chown kneo:kneo app.jar
USER kneo
EXPOSE 8080 38799
ENTRYPOINT ["java", "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", "-jar", "app.jar"]