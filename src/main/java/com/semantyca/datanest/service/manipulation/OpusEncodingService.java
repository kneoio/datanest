package com.semantyca.datanest.service.manipulation;

import com.semantyca.datanest.config.DatanestConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class OpusEncodingService {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpusEncodingService.class);
    private static final int TIMEOUT_MINUTES = 10;

    private final String ffmpegPath;

    @Inject
    public OpusEncodingService(DatanestConfig config) {
        this.ffmpegPath = config.getFfmpegPath();
    }

    public Path encode(Path inputFile, long bitRateKbps) throws IOException, InterruptedException {
        Path outputFile = Paths.get(inputFile.toString() + ".opus");
        String bitRate = bitRateKbps + "k";

        ProcessBuilder pb = new ProcessBuilder(
                ffmpegPath,
                "-y",
                "-i", inputFile.toString(),
                "-c:a", "libopus",
                "-b:a", bitRate,
                "-vn",
                outputFile.toString()
        );
        pb.redirectErrorStream(true);

        LOGGER.info("Encoding {} to opus at {}kbps -> {}", inputFile.getFileName(), bitRateKbps, outputFile.getFileName());
        Process process = pb.start();
        boolean finished = process.waitFor(TIMEOUT_MINUTES, TimeUnit.MINUTES);

        if (!finished) {
            process.destroyForcibly();
            throw new IOException("FFmpeg encoding timed out for: " + inputFile);
        }
        if (process.exitValue() != 0) {
            throw new IOException("FFmpeg encoding failed (exit " + process.exitValue() + ") for: " + inputFile);
        }

        return outputFile;
    }
}
