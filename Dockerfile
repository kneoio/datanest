FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y wget apt-transport-https gnupg && \
    wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" > /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && apt-get install -y temurin-21-jre ffmpeg && \
    rm -rf /var/lib/apt/lists/*
RUN groupadd -r kneo && useradd -r -g kneo kneo

RUN mkdir -p /app/controller-uploads /app/file-uploads /var/log/datanest \
    && chown -R kneo:kneo /app /var/log/datanest
WORKDIR /app
COPY target/datanest-*-runner.jar app.jar
RUN chown kneo:kneo app.jar
USER kneo
EXPOSE 8080 38799
ENTRYPOINT ["java", "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", "-jar", "app.jar"]