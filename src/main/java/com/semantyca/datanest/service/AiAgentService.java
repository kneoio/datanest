package com.semantyca.datanest.service;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.cnst.LanguageTag;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.LabelFlatDTO;
import com.semantyca.datanest.dto.aiagent.*;
import com.semantyca.datanest.repository.AiAgentRepository;
import com.semantyca.mixpla.model.aiagent.AiAgent;
import com.semantyca.mixpla.model.aiagent.LanguagePreference;
import com.semantyca.mixpla.model.aiagent.TTSSetting;
import com.semantyca.mixpla.model.aiagent.Voice;
import com.semantyca.mixpla.model.cnst.LlmType;
import com.semantyca.mixpla.model.filter.AiAgentFilter;
import com.semantyca.officeframe.dto.LabelDTO;
import com.semantyca.officeframe.service.LabelService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class AiAgentService extends AbstractService<AiAgent, AiAgentDTO> {
    private static final Logger LOGGER = Logger.getLogger(AiAgentService.class);

    private final AiAgentRepository repository;
    private final LabelService labelService;

    @Inject
    public AiAgentService(
            UserService userService,
            AiAgentRepository repository,
            LabelService labelService
    ) {
        super(userService);
        this.repository = repository;
        this.labelService = labelService;
    }

    public Uni<List<AiAgentDTO>> getAll(final int limit, final int offset, final IUser user) {
        return getAll(limit, offset, user, null);
    }

    public Uni<List<AiAgentDTO>> getAll(final int limit, final int offset, final IUser user, AiAgentFilter filter) {
        return repository.getAll(limit, offset, false, user, filter)
                .chain(list -> {
                    if (list.isEmpty()) return Uni.createFrom().item(List.of());
                    List<Uni<AiAgentDTO>> unis = list.stream()
                            .map(this::mapToDTO)
                            .collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    public Uni<Integer> getAllCount(final IUser user) {
        return getAllCount(user, null);
    }

    public Uni<Integer> getAllCount(final IUser user, AiAgentFilter filter) {
        return repository.getAllCount(user, false, filter);
    }

    public Uni<List<AiAgent>> getAll(final int limit, final int offset) {
        return repository.getAll(limit, offset, false, SuperUser.build());
    }

    public Uni<AiAgent> getById(UUID id, IUser user, LanguageCode language) {
        return repository.findById(id, user, false);
    }

    @Override
    public Uni<Integer> delete(String id, IUser user) {
        return repository.delete(UUID.fromString(id), user);
    }

    @Override
    public Uni<AiAgentDTO> getDTO(UUID id, IUser user, LanguageCode language) {
        return repository.findById(id, user, false).chain(this::mapToDTO);
    }

    public Uni<List<AiAgentFlatDTO>> getAllFlat(final int limit, final int offset, final IUser user) {
        return repository.getAll(limit, offset, false, user)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    }
                    List<Uni<AiAgentFlatDTO>> unis = list.stream()
                            .map(this::mapToFlatDTO)
                            .collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    public Uni<List<AiAgentFlatDTO>> getAllFlatByLabelIdentifiers(final int limit, final int offset, final IUser user, List<String> identifiers) {
        return repository.getAllByLabelIdentifiers(limit, offset, false, user, identifiers)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    }
                    List<Uni<AiAgentFlatDTO>> unis = list.stream()
                            .map(this::mapToFlatDTO)
                            .collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    public Uni<Integer> getAllCountByLabelIdentifiers(final IUser user, List<String> identifiers) {
        return repository.getAllCountByLabelIdentifiers(user, false, identifiers);
    }

    private Uni<AiAgentFlatDTO> mapToFlatDTO(AiAgent doc) {
        AiAgentFlatDTO dto = new AiAgentFlatDTO();
        dto.setId(doc.getId());
        dto.setName(doc.getName());
        dto.setDescription(doc.getDescription());
        dto.setManner(doc.getManner());
        if (doc.getPreferredLang() != null && !doc.getPreferredLang().isEmpty()) {
            List<LanguagePreferenceDTO> langPrefDTOs = doc.getPreferredLang().stream()
                    .map(pref -> new LanguagePreferenceDTO(pref.getLanguageTag().tag(), pref.getWeight()))
                    .toList();
            dto.setPreferredLang(langPrefDTOs);
        }
        if (doc.getLabels() != null && !doc.getLabels().isEmpty()) {
            List<Uni<LabelDTO>> labelUnis = doc.getLabels().stream()
                    .map(labelId -> labelService.getDTO(labelId, null, LanguageCode.en))
                    .collect(Collectors.toList());
            return Uni.join().all(labelUnis).andFailFast()
                    .map(labels -> {
                        dto.setLabels(labels.stream().map(LabelFlatDTO::from).toList());
                        return dto;
                    });
        }
        return Uni.createFrom().item(dto);
    }

    public Uni<AiAgentDTO> upsert(String id, AiAgentDTO dto, IUser user, LanguageCode code) {
        AiAgent entity = buildEntity(dto);
        List<RlsActionDTO> rlsActions = dto.getRlsActions() != null ? dto.getRlsActions() : List.of();
        if ("new".equalsIgnoreCase(id) || id == null || id.isBlank()) {
            return repository.insert(entity, rlsActions, user).chain(this::mapToDTO);
        } else {
            return repository.update(UUID.fromString(id), entity, rlsActions, user).chain(this::mapToDTO);
        }
    }

    private Uni<AiAgentDTO> mapToDTO(AiAgent doc) {
        return Uni.combine().all().unis(
                userService.getUserName(doc.getAuthor()),
                userService.getUserName(doc.getLastModifier())
        ).asTuple().map(tuple -> {
            AiAgentDTO dto = new AiAgentDTO();
            dto.setId(doc.getId());
            dto.setAuthor(tuple.getItem1());
            dto.setRegDate(doc.getRegDate());
            dto.setLastModifier(tuple.getItem2());
            dto.setLastModifiedDate(doc.getLastModifiedDate());
            dto.setName(doc.getName());
            dto.setDescription(doc.getDescription());
            dto.setManner(doc.getManner());
            dto.setLlmType(doc.getLlmType());

            if (doc.getPreferredLang() != null && !doc.getPreferredLang().isEmpty()) {
                List<LanguagePreferenceDTO> langPrefDTOs = doc.getPreferredLang().stream()
                        .map(pref -> new LanguagePreferenceDTO(pref.getLanguageTag().tag(), pref.getWeight()))
                        .toList();
                dto.setPreferredLang(langPrefDTOs);
            }

            if (doc.getCopilot() != null) dto.setCopilot(doc.getCopilot());

            if (doc.getTtsSetting() != null) {
                TTSSettingDTO ttsSettingDTO = new TTSSettingDTO();
                if (doc.getTtsSetting().getDj() != null) {
                    VoiceDTO djVoice = new VoiceDTO();
                    djVoice.setId(doc.getTtsSetting().getDj().getId());
                    djVoice.setName(doc.getTtsSetting().getDj().getName());
                    djVoice.setEngineType(doc.getTtsSetting().getDj().getEngineType());
                    djVoice.setGain(doc.getTtsSetting().getDj().getGain());
                    ttsSettingDTO.setDj(djVoice);
                }
                if (doc.getTtsSetting().getCopilot() != null) {
                    VoiceDTO copilotVoice = new VoiceDTO();
                    copilotVoice.setId(doc.getTtsSetting().getCopilot().getId());
                    copilotVoice.setName(doc.getTtsSetting().getCopilot().getName());
                    copilotVoice.setEngineType(doc.getTtsSetting().getCopilot().getEngineType());
                    copilotVoice.setGain(doc.getTtsSetting().getCopilot().getGain());
                    ttsSettingDTO.setCopilot(copilotVoice);
                }
                if (doc.getTtsSetting().getNewsReporter() != null) {
                    VoiceDTO newsReporterVoice = new VoiceDTO();
                    newsReporterVoice.setId(doc.getTtsSetting().getNewsReporter().getId());
                    newsReporterVoice.setName(doc.getTtsSetting().getNewsReporter().getName());
                    newsReporterVoice.setEngineType(doc.getTtsSetting().getNewsReporter().getEngineType());
                    newsReporterVoice.setGain(doc.getTtsSetting().getNewsReporter().getGain());
                    ttsSettingDTO.setNewsReporter(newsReporterVoice);
                }
                if (doc.getTtsSetting().getWeatherReporter() != null) {
                    VoiceDTO weatherReporterVoice = new VoiceDTO();
                    weatherReporterVoice.setId(doc.getTtsSetting().getWeatherReporter().getId());
                    weatherReporterVoice.setName(doc.getTtsSetting().getWeatherReporter().getName());
                    weatherReporterVoice.setEngineType(doc.getTtsSetting().getWeatherReporter().getEngineType());
                    weatherReporterVoice.setGain(doc.getTtsSetting().getWeatherReporter().getGain());
                    ttsSettingDTO.setWeatherReporter(weatherReporterVoice);
                }
                if (doc.getTtsSetting().getAdReader() != null) {
                    VoiceDTO adReaderVoice = new VoiceDTO();
                    adReaderVoice.setId(doc.getTtsSetting().getAdReader().getId());
                    adReaderVoice.setName(doc.getTtsSetting().getAdReader().getName());
                    adReaderVoice.setEngineType(doc.getTtsSetting().getAdReader().getEngineType());
                    adReaderVoice.setGain(doc.getTtsSetting().getAdReader().getGain());
                    ttsSettingDTO.setAdReader(adReaderVoice);
                }
                dto.setTtsSetting(ttsSettingDTO);
            }

            if (doc.getLabels() != null && !doc.getLabels().isEmpty()) {
                dto.setLabels(doc.getLabels());
            }

            return dto;
        });
    }

    private AiAgent buildEntity(AiAgentDTO dto) {
        AiAgent doc = new AiAgent();
        doc.setId(dto.getId());
        doc.setName(dto.getName());
        doc.setDescription(dto.getDescription());
        doc.setManner(dto.getManner());
        doc.setCopilot(dto.getCopilot());
        
        if (dto.getLabels() != null && !dto.getLabels().isEmpty()) {
            doc.setLabels(dto.getLabels());
        } else {
            doc.setLabels(new ArrayList<>());
        }

        doc.setLlmType(dto.getLlmType() != null ? dto.getLlmType() : LlmType.CLAUDE);
        
        if (dto.getPreferredLang() != null && !dto.getPreferredLang().isEmpty()) {
            List<LanguagePreference> langPrefs = dto.getPreferredLang().stream()
                    .map(prefDto -> {
                            LanguagePreference pref = new LanguagePreference();
                            pref.setWeight(prefDto.getWeight());
                            pref.setLanguageTag(LanguageTag.fromTag(prefDto.getLanguageTag()));
                            return pref;
                    })
                    .collect(Collectors.toList());
            doc.setPreferredLang(langPrefs);
        }

        if (dto.getTtsSetting() != null) {
            TTSSetting ttsSetting = new TTSSetting();
            if (dto.getTtsSetting().getDj() != null) {
                Voice djVoice = new Voice();
                djVoice.setId(dto.getTtsSetting().getDj().getId());
                djVoice.setName(dto.getTtsSetting().getDj().getName());
                djVoice.setEngineType(dto.getTtsSetting().getDj().getEngineType());
                djVoice.setGain(dto.getTtsSetting().getDj().getGain());
                ttsSetting.setDj(djVoice);
            }
            if (dto.getTtsSetting().getCopilot() != null) {
                Voice copilotVoice = new Voice();
                copilotVoice.setId(dto.getTtsSetting().getCopilot().getId());
                copilotVoice.setName(dto.getTtsSetting().getCopilot().getName());
                copilotVoice.setEngineType(dto.getTtsSetting().getCopilot().getEngineType());
                copilotVoice.setGain(dto.getTtsSetting().getCopilot().getGain());
                ttsSetting.setCopilot(copilotVoice);
            }
            if (dto.getTtsSetting().getNewsReporter() != null) {
                Voice newsReporterVoice = new Voice();
                newsReporterVoice.setId(dto.getTtsSetting().getNewsReporter().getId());
                newsReporterVoice.setName(dto.getTtsSetting().getNewsReporter().getName());
                newsReporterVoice.setEngineType(dto.getTtsSetting().getNewsReporter().getEngineType());
                newsReporterVoice.setGain(dto.getTtsSetting().getNewsReporter().getGain());
                ttsSetting.setNewsReporter(newsReporterVoice);
            }
            if (dto.getTtsSetting().getWeatherReporter() != null) {
                Voice weatherReporterVoice = new Voice();
                weatherReporterVoice.setId(dto.getTtsSetting().getWeatherReporter().getId());
                weatherReporterVoice.setName(dto.getTtsSetting().getWeatherReporter().getName());
                weatherReporterVoice.setEngineType(dto.getTtsSetting().getWeatherReporter().getEngineType());
                weatherReporterVoice.setGain(dto.getTtsSetting().getWeatherReporter().getGain());
                ttsSetting.setWeatherReporter(weatherReporterVoice);
            }
            if (dto.getTtsSetting().getAdReader() != null) {
                Voice adReaderVoice = new Voice();
                adReaderVoice.setId(dto.getTtsSetting().getAdReader().getId());
                adReaderVoice.setName(dto.getTtsSetting().getAdReader().getName());
                adReaderVoice.setEngineType(dto.getTtsSetting().getAdReader().getEngineType());
                adReaderVoice.setGain(dto.getTtsSetting().getAdReader().getGain());
                ttsSetting.setAdReader(adReaderVoice);
            }
            doc.setTtsSetting(ttsSetting);
        }

        return doc;
    }

    public Uni<List<DocumentAccessDTO>> getDocumentAccess(UUID documentId, IUser user) {
        return repository.getDocumentAccessInfo(documentId, user)
                .onItem().transform(accessInfoList ->
                        accessInfoList.stream()
                                .map(this::mapToDocumentAccessDTO)
                                .collect(Collectors.toList())
                );
    }
}
