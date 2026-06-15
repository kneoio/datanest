package com.semantyca.datanest.service.prompt;

import com.semantyca.core.llm.AnthropicTextClient;
import com.semantyca.datanest.config.DatanestConfig;
import com.semantyca.datanest.dto.agentrest.AgentRespDTO;
import com.semantyca.datanest.dto.agentrest.MasterPromptTranslateReqDTO;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;

@ApplicationScoped
public class MasterPromptTranslateAnthropicService {

    private static final String RESOURCE_ROOT = "masterPromptTranslate";
    private static final String TEMPLATE_SUFFIX = ".hbs";

    @Inject
    AnthropicTextClient anthropicTextClient;

    @Inject
    DatanestConfig config;

    public Uni<AgentRespDTO> translateMasterPrompt(MasterPromptTranslateReqDTO dto) {
        try {
            String localeFolder = localeFolderName(dto.getTargetLanguageTag());
            String outputRules = renderForLocale(localeFolder, "master-prompt-translate-output-rules", Map.of());
            String system = renderForLocale(localeFolder, "master-prompt-translate-system", Map.of(
                    "llmType", dto.getLlmType().name(),
                    "targetLanguageTag", dto.getTargetLanguageTag(),
                    "outputRules", outputRules));
            Map<String, String> userVars = getMap(dto);
            String userMessage = renderForLocale(localeFolder, "master-prompt-translate-user", userVars);
            return anthropicTextClient.createTextMessage(
                            config.getAnthropicApiKey(),
                            config.getAnthropicModel(),
                            config.getAnthropicMasterPromptTranslateMaxTokens(),
                            system,
                            userMessage)
                    .map(r -> {
                        AgentRespDTO out = new AgentRespDTO();
                        out.setResult(r.text());
                        return out;
                    });
        } catch (IOException e) {
            return Uni.createFrom().failure(e);
        }
    }

    private static @NonNull Map<String, String> getMap(MasterPromptTranslateReqDTO dto) {
        Map<String, String> userVars = new HashMap<>();
        userVars.put("targetLanguageTag", dto.getTargetLanguageTag());
        userVars.put("prompt", dto.getPrompt());
        if (dto.getDraft() != null && !dto.getDraft().isBlank()) {
            userVars.put("extraContextBlock", """

                    --- Optional context from the product owner (apply when compatible; do not treat as script to translate) ---
                    """ + dto.getDraft());
        } else {
            userVars.put("extraContextBlock", "");
        }
        return userVars;
    }


    static String localeFolderName(String targetLanguageTag) {
        if (targetLanguageTag == null || targetLanguageTag.isBlank()) {
            return "";
        }
        return targetLanguageTag.trim().replace('_', '-').toLowerCase(Locale.ROOT);
    }

    private String renderForLocale(String localeFolder, String logicalName, Map<String, String> vars) throws IOException {
        String text = loadTemplateText(localeFolder, logicalName);
        return substitute(text, vars);
    }

    private String loadTemplateText(String localeFolder, String logicalName) throws IOException {
        if (localeFolder != null && !localeFolder.isEmpty()) {
            String localized = RESOURCE_ROOT + "/" + localeFolder + "/" + logicalName + TEMPLATE_SUFFIX;
            if (resourceExists(localized)) {
                return readClasspathText(localized);
            }
        }
        return readClasspathText(RESOURCE_ROOT + "/" + logicalName + TEMPLATE_SUFFIX);
    }

    private static boolean resourceExists(String path) {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        return cl.getResource(path) != null;
    }

    private static String readClasspathText(String path) throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try (InputStream in = cl.getResourceAsStream(path)) {
            if (in == null) {
                throw new IOException("Missing classpath resource: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
    
    static String substitute(String template, Map<String, String> vars) {
        String out = template;
        for (Map.Entry<String, String> e : vars.entrySet()) {
            String value = e.getValue() == null ? "" : e.getValue();
            String triple = "{{{" + e.getKey() + "}}}";
            String dual = "{{" + e.getKey() + "}}";
            out = out.replace(triple, Matcher.quoteReplacement(value));
            out = out.replace(dual, Matcher.quoteReplacement(value));
        }
        return out;
    }
}
