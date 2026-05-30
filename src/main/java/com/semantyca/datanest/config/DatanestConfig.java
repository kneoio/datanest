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

    @WithName("controller.upload.files.path")
    @WithDefault("controller-uploads")
    String getPathUploads();


    @WithName("ffprobe.path")
    @WithDefault("ffprobe")
    String getFfprobePath();

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

    @WithName("genre.other-id")
    Optional<String> getOtherGenreId();

}