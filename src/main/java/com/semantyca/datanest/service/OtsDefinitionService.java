package com.semantyca.datanest.service;

import com.semantyca.core.llm.AnthropicTextClient;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.util.WebHelper;
import com.semantyca.datanest.config.DatanestConfig;
import com.semantyca.datanest.dto.OtsDefinitionDTO;
import com.semantyca.datanest.repository.OtsDefinitionRepository;
import com.semantyca.mixpla.model.Script;
import com.semantyca.mixpla.model.filter.OtsDefinitionFilter;
import com.semantyca.mixpla.model.stream.OtsDefinition;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class OtsDefinitionService {
    private static final long NAME_MAX_TOKENS = 30;
    private static final String NAME_SYSTEM_PROMPT = """
            You generate short, friendly display names for a listener's personal radio stream, \
            based on a script template and the values the listener filled in. \
            Reply with the name only - no quotes, no explanation, 2 to 6 words, title case.""";

    private final OtsDefinitionRepository repository;
    private final ScriptService scriptService;
    private final AnthropicTextClient anthropicTextClient;
    private final DatanestConfig config;

    protected OtsDefinitionService() {
        this.repository = null;
        this.scriptService = null;
        this.anthropicTextClient = null;
        this.config = null;
    }

    @Inject
    public OtsDefinitionService(OtsDefinitionRepository repository,
                                 ScriptService scriptService,
                                 AnthropicTextClient anthropicTextClient,
                                 DatanestConfig config) {
        this.repository = repository;
        this.scriptService = scriptService;
        this.anthropicTextClient = anthropicTextClient;
        this.config = config;
    }

    public Uni<List<OtsDefinitionDTO>> getAllDTO(final int limit, final int offset, final IUser user, final OtsDefinitionFilter filter) {
        assert repository != null;
        return repository.getAll(limit, offset, false, user, filter)
                .map(list -> list.stream().map(this::mapToDTO).collect(Collectors.toList()));
    }

    public Uni<Integer> getAllCount(final IUser user, final OtsDefinitionFilter filter) {
        assert repository != null;
        return repository.getAllCount(user, false, filter);
    }

    public Uni<OtsDefinitionDTO> getDTO(UUID id, IUser user) {
        assert repository != null;
        return repository.findById(id, user).map(this::mapToDTO);
    }

    public Uni<OtsDefinitionDTO> upsert(String id, OtsDefinitionDTO dto, IUser user) {
        if (dto.getBrandId() == null && dto.getAgentId() == null) {
            return Uni.createFrom().failure(
                    new IllegalArgumentException("agentId is required when brandId is not set"));
        }
        if ("new".equalsIgnoreCase(id) || id == null || id.isBlank()) {
            return create(dto, user);
        }
        return update(UUID.fromString(id), dto, user);
    }

    public Uni<Integer> archive(String id, IUser user) {
        assert repository != null;
        return repository.archive(UUID.fromString(id), user);
    }

    private Uni<OtsDefinitionDTO> update(UUID id, OtsDefinitionDTO dto, IUser user) {
        assert repository != null;
        OtsDefinition entity = new OtsDefinition();
        entity.setName(dto.getName());
        entity.setScriptId(dto.getScriptId());
        entity.setUserVariables(dto.getUserVariables());
        entity.setBrandId(dto.getBrandId());
        entity.setAgentId(dto.getAgentId());
        return repository.update(id, entity, user).map(this::mapToDTO);
    }

    private Uni<OtsDefinitionDTO> create(OtsDefinitionDTO dto, IUser user) {
        assert repository != null;
        assert scriptService != null;

        return scriptService.getById(dto.getScriptId(), user)
                .chain(script -> generateName(script, dto.getUserVariables())
                        .chain(name -> generateUniqueSlug(name)
                                .chain(slug -> {
                                    OtsDefinition entity = new OtsDefinition();
                                    entity.setName(name);
                                    entity.setSlugName(slug);
                                    entity.setScriptId(dto.getScriptId());
                                    entity.setUserVariables(dto.getUserVariables());
                                    entity.setBrandId(dto.getBrandId());
                                    entity.setAgentId(dto.getAgentId());
                                    return repository.insert(entity, user);
                                })
                        )
                )
                .map(this::mapToDTO);
    }

    private Uni<String> generateName(Script script, Map<String, Object> userVariables) {
        assert anthropicTextClient != null;
        assert config != null;

        String variablesText = userVariables == null || userVariables.isEmpty()
                ? "(no variables)"
                : userVariables.entrySet().stream()
                        .map(e -> e.getKey() + ": " + e.getValue())
                        .collect(Collectors.joining("\n"));

        String userMessage = "Script name: " + script.getName() + "\nVariables:\n" + variablesText;

        return anthropicTextClient.createTextMessage(
                        config.getAnthropicApiKey(),
                        config.getAnthropicModel(),
                        NAME_MAX_TOKENS,
                        NAME_SYSTEM_PROMPT,
                        userMessage)
                .map(result -> result.text().trim());
    }

    private Uni<String> generateUniqueSlug(String name) {
        String baseSlug = WebHelper.generateSlug(name);
        return tryUniqueSlug(baseSlug, 0);
    }

    private Uni<String> tryUniqueSlug(String baseSlug, int attempt) {
        assert repository != null;
        String candidate = attempt == 0 ? baseSlug : baseSlug + "-" + UUID.randomUUID().toString().substring(0, 6);
        return repository.existsBySlug(candidate)
                .chain(exists -> exists
                        ? tryUniqueSlug(baseSlug, attempt + 1)
                        : Uni.createFrom().item(candidate));
    }

    private OtsDefinitionDTO mapToDTO(OtsDefinition ots) {
        OtsDefinitionDTO dto = new OtsDefinitionDTO();
        dto.setId(ots.getId());
        dto.setName(ots.getName());
        dto.setSlugName(ots.getSlugName());
        dto.setScriptId(ots.getScriptId());
        dto.setUserVariables(ots.getUserVariables());
        dto.setBrandId(ots.getBrandId());
        dto.setAgentId(ots.getAgentId());
        return dto;
    }
}
