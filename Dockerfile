FROM eclipse-temurin:21-jre-jammy
RUN groupadd -r kneo && useradd -r -g kneo kneo

WORKDIR /app
COPY third_party/ffmpeg/linux_x86_64/ffmpeg /usr/bin/ffmpeg
COPY third_party/ffmpeg/linux_x86_64/ffprobe /usr/bin/ffprobe
RUN chmod +x /usr/bin/ffmpeg /usr/bin/ffprobe
COPY target/datanest-1.0.0-SNAPSHOT-runner.jar app.jar
RUN chown kneo:kneo app.jar
USER kneo
EXPOSE 8080 38799
ENTRYPOINT ["java", "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", "-jar", "app.jar"]