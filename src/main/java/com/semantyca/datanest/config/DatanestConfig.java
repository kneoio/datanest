package com.semantyca.datanest.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

import java.util.Optional;

@ConfigMapping(prefix = "datanest")
public interface DatanestConfig {
    @WithName("host")
    @WithDefault("localhost")
    String getHost();

    @WithName("agent.url")
    @WithDefault("http://localhost:38799")
    String getAgentUrl();

    @WithName("spectra.url")
    @WithDefault("http://127.0.0.1:38795")
    String getSpectraUrl();

    @WithName("controller.upload.files.path")
    @WithDefault("controller-uploads")
    String getPathUploads();


    @WithName("ffprobe.path")
    @WithDefault("ffprobe")
    String getFfprobePath();

    @WithName("ffmpeg.path")
    @WithDefault("ffmpeg")
    String getFfmpegPath();

    @WithName("groq.api-key")
    Optional<String> getGroqApiKey();

    @WithName("anthropic.api-key")
    String getAnthropicApiKey();

    @WithName("anthropic.model")
    @WithDefault("claude-sonnet-4-20250514")
    String getAnthropicModel();

    @WithName("anthropic.master-prompt-translate.max-tokens")
    @WithDefault("4096")
    long getAnthropicMasterPromptTranslateMaxTokens();

    @WithName("openai.api-key")
    Optional<String> getOpenAiApiKey();

    @WithName("openai.model")
    @WithDefault("gpt-4o")
    String getOpenAiModel();

    @WithName("openai.master-prompt-translate.max-tokens")
    @WithDefault("4096")
    long getOpenAiMasterPromptTranslateMaxTokens();

    @WithName("genre.other-id")
    Optional<String> getOtherGenreId();

    @WithName("otp.test-bypass.email")
    @WithDefault("")
    String getOtpTestBypassEmail();

    @WithName("otp.test-bypass.code")
    @WithDefault("")
    String getOtpTestBypassCode();

}