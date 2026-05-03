package com.semantyca.datanest.service;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.dto.scheduler.OnceTriggerDTO;
import com.semantyca.core.dto.scheduler.PeriodicTriggerDTO;
import com.semantyca.core.dto.scheduler.ScheduleDTO;
import com.semantyca.core.dto.scheduler.ScheduleFactory;
import com.semantyca.core.dto.scheduler.TaskDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.cnst.TriggerType;
import com.semantyca.core.model.scheduler.OnceTrigger;
import com.semantyca.core.model.scheduler.PeriodicTrigger;
import com.semantyca.core.model.scheduler.Scheduler;
import com.semantyca.core.model.scheduler.Task;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.ScenePromptDTO;
import com.semantyca.datanest.dto.StagePlaylistDTO;
import com.semantyca.datanest.dto.RlsActionDTO;
import com.semantyca.datanest.dto.event.EventDTO;
import com.semantyca.datanest.dto.event.EventEntryDTO;
import com.semantyca.datanest.external.PerplexityApiClient;
import com.semantyca.datanest.repository.EventRepository;
import com.semantyca.mixpla.model.Event;
import com.semantyca.mixpla.model.PlaylistRequest;
import com.semantyca.mixpla.model.ScenePrompt;
import com.semantyca.mixpla.model.brand.Brand;
import com.semantyca.mixpla.model.cnst.EventPriority;
import com.semantyca.mixpla.model.cnst.EventType;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import com.semantyca.mixpla.model.cnst.SourceType;
import com.semantyca.mixpla.model.cnst.WayOfSourcing;
import com.semantyca.officeframe.model.cnst.CountryCode;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class EventService extends AbstractService<Event, EventDTO> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventService.class);
    private final EventRepository repository;
    private final BrandService brandService;
    private final PerplexityApiClient perplexityApiClient;

    @Inject
    public EventService(UserService userService,
                        EventRepository repository,
                        BrandService brandService,
                        PerplexityApiClient perplexityApiClient
    ) {
        super(userService);
        this.repository = repository;
        this.brandService = brandService;
        this.perplexityApiClient = perplexityApiClient;
    }

    public Uni<List<EventEntryDTO>> getAll(final int limit, final int offset, final IUser user) {
        assert repository != null;
        return repository.getAll(limit, offset, false, user)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    } else {
                        List<Uni<EventEntryDTO>> unis = list.stream()
                                .map(this::mapToEntryDTO)
                                .collect(Collectors.toList());
                        return Uni.join().all(unis).andFailFast();
                    }
                });
    }

    public Uni<Integer> getAllCount(final IUser user) {
        assert repository != null;
        return repository.getAllCount(user, false);
    }

    @Override
    public Uni<EventDTO> getDTO(UUID uuid, IUser user, LanguageCode code) {
        assert repository != null;
        return repository.findById(uuid, user, false)
                .chain(this::mapToDTO);
    }

    public Uni<List<EventDTO>> getForBrand(String brandSlugName, int limit, final int offset, IUser user) {
        assert repository != null;
        return repository.findForBrand(brandSlugName, limit, offset, user, false)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    } else {
                        List<Uni<EventDTO>> unis = list.stream()
                                .map(this::mapToDTO)
                                .collect(Collectors.toList());
                        return Uni.join().all(unis).andFailFast();
                    }
                });
    }

    public Uni<Integer> getCountForBrand(final String brandSlugName, final IUser user) {
        assert repository != null;
        return repository.findForBrandCount(brandSlugName, user, false);
    }

    public Uni<EventDTO> upsert(String id, EventDTO dto, IUser user) {
        assert repository != null;

        Event entity = buildEntity(dto);
        List<RlsActionDTO> rlsActions = dto.getRlsActions() != null ? dto.getRlsActions() : List.of();

        Uni<Event> saveOperation;
        if ("new".equalsIgnoreCase(id) || id == null || id.isBlank()) {
            saveOperation = repository.insert(entity, rlsActions, user);
        } else {
            saveOperation = repository.update(UUID.fromString(id), entity, rlsActions, user);
        }

        return saveOperation.chain(this::mapToDTO);
    }

    public Uni<Integer> archive(String id, IUser user) {
        assert repository != null;
        return repository.archive(UUID.fromString(id), user);
    }

    public Uni<List<EventDTO>> generateCountryEventsFromPerplexity(String brandSlugName, IUser user) {
        return brandService.getBySlugNameForUser(brandSlugName, user)
                .onItem().transformToUni(brand -> {
                    CountryCode country = brand.getCountry();
                    if (country == null || country == CountryCode.UNKNOWN) {
                        return Uni.createFrom().failure(
                                new IllegalArgumentException("Brand has no country assigned"));
                    }
                    LanguageCode lang = country.getPreferredLanguage();
                    if (lang == null || lang == LanguageCode.unknown) {
                        lang = LanguageCode.en;
                    }
                    String query = getQuery(country);
                    return perplexityApiClient.search(query, List.of(lang), List.of(), true)
                            .onItem().transformToUni(response -> persistEventsFromPerplexityArticles(response, brand, user));
                });
    }

    private static @NonNull String getQuery(CountryCode country) {
        String countryLabel = country.getCountryName();
        return "Research major public events, national holidays, elections, sports "
                + "championships, and cultural happenings in " + countryLabel
                + " that are relevant for broadcast radio audiences in the recent past and near "
                + "future. Include approximate dates where known. Populate the response schema: "
                + "provide an overall title and summary, and list each distinct real-world event as "
                + "a separate article entry with title, date, source, url when available, and a short "
                + "content description.";
    }

    private Uni<List<EventDTO>> persistEventsFromPerplexityArticles(JsonObject response, Brand brand, IUser user) {
        JsonArray articles = response.getJsonArray("articles");
        List<Uni<EventDTO>> saves = new ArrayList<>();
        if (articles != null && !articles.isEmpty()) {
            int cap = Math.min(articles.size(), 25);
            for (int i = 0; i < cap; i++) {
                JsonObject article = articles.getJsonObject(i);
                if (article == null) {
                    continue;
                }
                EventDTO dto = buildGeneratedEventDto(brand, article, response);
                if (dto.getDescription() != null && !dto.getDescription().isBlank()) {
                    saves.add(upsert("new", dto, user));
                }
            }
        }
        if (saves.isEmpty()) {
            EventDTO fallback = buildGeneratedEventDto(brand, null, response);
            if (fallback.getDescription() == null || fallback.getDescription().isBlank()) {
                return Uni.createFrom().item(List.of());
            }
            return upsert("new", fallback, user).map(List::of);
        }
        return Uni.join().all(saves).andFailFast();
    }

    private EventDTO buildGeneratedEventDto(Brand brand, JsonObject article, JsonObject envelope) {
        EventDTO dto = new EventDTO();
        dto.setBrandId(brand.getId().toString());
        dto.setType(EventType.SPECIAL.name());
        dto.setPriority(EventPriority.MEDIUM.name());
        ZoneId tz = brand.getTimeZone() != null ? brand.getTimeZone() : ZoneId.of("UTC");
        dto.setTimeZone(tz.getId());

        String title;
        String body;
        if (article != null) {
            title = article.getString("title");
            String content = article.getString("content");
            String date = article.getString("date");
            String source = article.getString("source");
            String url = article.getString("url");
            StringBuilder desc = new StringBuilder();
            if (title != null && !title.isBlank()) {
                desc.append(title.trim());
            }
            if (date != null && !date.isBlank()) {
                if (!desc.isEmpty()) {
                    desc.append("\n\n");
                }
                desc.append("Date: ").append(date.trim());
            }
            if (content != null && !content.isBlank()) {
                if (!desc.isEmpty()) {
                    desc.append("\n\n");
                }
                desc.append(content.trim());
            }
            if (source != null && !source.isBlank()) {
                if (!desc.isEmpty()) {
                    desc.append("\n\n");
                }
                desc.append("Source: ").append(source.trim());
            }
            if (url != null && !url.isBlank()) {
                if (!desc.isEmpty()) {
                    desc.append("\n");
                }
                desc.append(url.trim());
            }
            body = desc.toString();
        } else {
            title = envelope.getString("title");
            String summary = envelope.getString("summary");
            StringBuilder desc = new StringBuilder();
            if (title != null && !title.isBlank()) {
                desc.append(title.trim());
            }
            if (summary != null && !summary.isBlank()) {
                if (!desc.isEmpty()) {
                    desc.append("\n\n");
                }
                desc.append(summary.trim());
            }
            body = desc.toString();
        }
        dto.setDescription(body.isBlank() ? null : body);

        StagePlaylistDTO playlistDTO = new StagePlaylistDTO();
        playlistDTO.setSourcing(WayOfSourcing.RANDOM.toString());
        dto.setStagePlaylist(playlistDTO);
        dto.setSchedule(ScheduleFactory.createWorkdaySchedule(60));
        dto.setActions(List.of());
        return dto;
    }

    @Override
    public Uni<Integer> delete(String id, IUser user) {
        assert repository != null;
        return repository.delete(UUID.fromString(id), user);
    }

    private Uni<EventEntryDTO> mapToEntryDTO(Event doc) {
        assert brandService != null;
        return brandService.getById(doc.getBrandId(), SuperUser.build())
                .onItem().transform(Brand::getSlugName)
                .onFailure().recoverWithItem("Unknown Brand")
                .map(brand -> new EventEntryDTO(
                        doc.getId(),
                        brand,
                        doc.getType().name(),
                        doc.getPriority().name(),
                        doc.getDescription()
                ));
    }

    private Uni<EventDTO> mapToDTO(Event doc) {
        assert brandService != null;
        return Uni.combine().all().unis(
                userService.getUserName(doc.getAuthor()),
                userService.getUserName(doc.getLastModifier()),
                brandService.getById(doc.getBrandId(), SuperUser.build())
        ).asTuple().map(tuple -> {
            EventDTO dto = new EventDTO();
            dto.setId(doc.getId());
            dto.setAuthor(tuple.getItem1());
            dto.setRegDate(doc.getRegDate());
            dto.setLastModifier(tuple.getItem2());
            dto.setLastModifiedDate(doc.getLastModifiedDate());
            dto.setBrandId(tuple.getItem3().getId().toString());
            //dto.setBrand(tuple.getItem3().getSlugName());
            dto.setType(doc.getType().name());
            dto.setDescription(doc.getDescription());
            dto.setPriority(doc.getPriority().name());
            dto.setActions(mapActionsToDTOs(doc.getScenePrompts()));
            dto.setStagePlaylist(mapStagePlaylistToDTO(doc.getPlaylistRequest()));

            if (doc.getScheduler() != null) {
                ScheduleDTO scheduleDTO = new ScheduleDTO();
                Scheduler schedule = doc.getScheduler();
                scheduleDTO.setEnabled(schedule.isEnabled());
                if (schedule.isEnabled() && schedule.getTasks() != null && !schedule.getTasks().isEmpty()) {
                    List<TaskDTO> taskDTOs = schedule.getTasks().stream().map(task -> {
                        TaskDTO taskDTO = new TaskDTO();
                        taskDTO.setId(task.getId());
                        taskDTO.setTriggerType(task.getTriggerType());
                        dto.setTimeZone(schedule.getTimeZone().getId());

                        if (task.getTriggerType() == TriggerType.ONCE) {
                            OnceTriggerDTO onceTriggerDTO = new OnceTriggerDTO();
                            onceTriggerDTO.setStartTime(task.getOnceTrigger().getStartTime());
                            onceTriggerDTO.setDuration(task.getOnceTrigger().getDuration());
                            onceTriggerDTO.setWeekdays(task.getOnceTrigger().getWeekdays());
                            taskDTO.setOnceTrigger(onceTriggerDTO);
                        }

                        if (task.getTriggerType() == TriggerType.PERIODIC) {
                            PeriodicTriggerDTO periodicTriggerDTO = new PeriodicTriggerDTO();
                            PeriodicTrigger trigger = task.getPeriodicTrigger();
                            periodicTriggerDTO.setStartTime(trigger.getStartTime());
                            periodicTriggerDTO.setEndTime(trigger.getEndTime());
                            periodicTriggerDTO.setWeekdays(trigger.getWeekdays());
                            periodicTriggerDTO.setInterval(trigger.getInterval());
                            taskDTO.setPeriodicTrigger(periodicTriggerDTO);
                        }

                        return taskDTO;
                    }).collect(Collectors.toList());

                    scheduleDTO.setTasks(taskDTOs);
                }
                dto.setSchedule(scheduleDTO);
            }
            return dto;
        });
    }

    private String normalizeTimeString(String timeString) {
        if ("24:00".equals(timeString)) {
            return "00:00";
        }
        return timeString;
    }

    private Event buildEntity(EventDTO dto) {
        Event doc = new Event();
        doc.setBrandId(UUID.fromString(dto.getBrandId()));
        doc.setType(EventType.valueOf(dto.getType()));
        doc.setDescription(dto.getDescription());
        doc.setPriority(EventPriority.valueOf(dto.getPriority()));
        doc.setTimeZone(ZoneId.of(dto.getTimeZone()));
        if (dto.getSchedule() != null) {
            Scheduler schedule = new Scheduler();
            ScheduleDTO scheduleDTO = dto.getSchedule();
            schedule.setTimeZone(doc.getTimeZone());
            schedule.setEnabled(scheduleDTO.isEnabled());
            if (scheduleDTO.getTasks() != null && !scheduleDTO.getTasks().isEmpty()) {
                List<Task> tasks = scheduleDTO.getTasks().stream().map(taskDTO -> {
                    Task task = new Task();
                    task.setId(UUID.randomUUID());
                    task.setTriggerType(taskDTO.getTriggerType());

                    if (taskDTO.getTriggerType() == TriggerType.ONCE) {
                        OnceTrigger onceTrigger = new OnceTrigger();
                        OnceTriggerDTO onceTriggerDTO = taskDTO.getOnceTrigger();
                        onceTrigger.setStartTime(normalizeTimeString(onceTriggerDTO.getStartTime()));
                        onceTrigger.setDuration(onceTriggerDTO.getDuration());
                        onceTrigger.setWeekdays(onceTriggerDTO.getWeekdays());
                        task.setOnceTrigger(onceTrigger);
                    }

                    if (taskDTO.getTriggerType() == TriggerType.PERIODIC) {
                        PeriodicTrigger periodicTrigger = new PeriodicTrigger();
                        PeriodicTriggerDTO periodicTriggerDTO = taskDTO.getPeriodicTrigger();
                        periodicTrigger.setStartTime(normalizeTimeString(periodicTriggerDTO.getStartTime()));
                        periodicTrigger.setEndTime(normalizeTimeString(periodicTriggerDTO.getEndTime()));
                        periodicTrigger.setInterval(periodicTriggerDTO.getInterval());
                        periodicTrigger.setWeekdays(periodicTriggerDTO.getWeekdays());
                        task.setPeriodicTrigger(periodicTrigger);
                    }

                    return task;
                }).collect(Collectors.toList());
                schedule.setTasks(tasks);
            }
            doc.setScheduler(schedule);
        }
        doc.setScenePrompts(mapDTOsToActions(dto.getActions()));
        doc.setPlaylistRequest(mapDTOToStagePlaylist(dto.getStagePlaylist()));

        return doc;
    }
    private List<ScenePromptDTO> mapActionsToDTOs(List<ScenePrompt> scenePrompts) {
        if (scenePrompts == null) {
            return null;
        }
        return scenePrompts.stream()
                .map(action -> {
                    ScenePromptDTO dto = new ScenePromptDTO();
                    dto.setPromptId(action.getPromptId());
                    dto.setRank(action.getRank());
                    dto.setWeight(action.getWeight());
                    dto.setActive(action.isActive());
                    return dto;
                })
                .collect(Collectors.toList());
    }

    private List<ScenePrompt> mapDTOsToActions(List<ScenePromptDTO> dtos) {
        if (dtos == null) {
            return List.of();
        }
        return dtos.stream()
                .map(dto -> {
                    ScenePrompt scenePrompt = new ScenePrompt();
                    scenePrompt.setPromptId(dto.getPromptId());
                    scenePrompt.setRank(dto.getRank());
                    scenePrompt.setWeight(dto.getWeight());
                    scenePrompt.setActive(dto.isActive());
                    return scenePrompt;
                })
                .collect(Collectors.toList());
    }

    private StagePlaylistDTO mapStagePlaylistToDTO(PlaylistRequest playlistRequest) {
        if (playlistRequest == null) {
            return null;
        }
        StagePlaylistDTO dto = new StagePlaylistDTO();
        dto.setSourcing(playlistRequest.getSourcing() != null ? playlistRequest.getSourcing().name() : null);
        dto.setTitle(playlistRequest.getTitle());
        dto.setArtist(playlistRequest.getArtist());
        dto.setGenres(playlistRequest.getGenres());
        dto.setLabels(playlistRequest.getLabels());
        dto.setType(playlistRequest.getType() != null ? playlistRequest.getType().stream().map(Enum::name).toList() : null);
        dto.setSource(playlistRequest.getSource() != null ? playlistRequest.getSource().stream().map(Enum::name).toList() : null);
        dto.setSearchTerm(playlistRequest.getSearchTerm());
        dto.setSoundFragments(playlistRequest.getSoundFragments());
        return dto;
    }

    private PlaylistRequest mapDTOToStagePlaylist(StagePlaylistDTO dto) {
        if (dto == null) {
            return null;
        }
        PlaylistRequest playlistRequest = new PlaylistRequest();
        playlistRequest.setSourcing(dto.getSourcing() != null ? WayOfSourcing.valueOf(dto.getSourcing()) : null);
        playlistRequest.setTitle(dto.getTitle());
        playlistRequest.setArtist(dto.getArtist());
        playlistRequest.setGenres(dto.getGenres());
        playlistRequest.setLabels(dto.getLabels());
        playlistRequest.setType(dto.getType() != null ? dto.getType().stream().map(PlaylistItemType::valueOf).toList() : null);
        playlistRequest.setSource(dto.getSource() != null ? dto.getSource().stream().map(SourceType::valueOf).toList() : null);
        playlistRequest.setSearchTerm(dto.getSearchTerm());
        playlistRequest.setSoundFragments(dto.getSoundFragments());
        return playlistRequest;
    }

    public Uni<List<DocumentAccessDTO>> getDocumentAccess(UUID documentId, IUser user) {
        assert repository != null;
        return repository.getDocumentAccessInfo(documentId, user)
                .onItem().transform(accessInfoList ->
                        accessInfoList.stream()
                                .map(this::mapToDocumentAccessDTO)
                                .collect(Collectors.toList())
                );
    }
}
