package com.semantyca.datanest.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

import java.util.List;

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


    @WithName("agent.api-key")
    String getAgentApiKey();



}