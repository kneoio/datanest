package com.semantyca.datanest.external;

import io.smallrye.mutiny.Uni;

@Deprecated
public interface LlmTextClient {
    Uni<LlmTextResult> createTextMessage(String model, long maxTokens, String systemPrompt, String userMessage);
}
