package com.semantyca.datanest.service;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.dto.scheduler.OnceTriggerDTO;
import com.semantyca.core.dto.scheduler.PeriodicTriggerDTO;
import com.semantyca.core.dto.scheduler.ScheduleDTO;
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
import com.semantyca.datanest.dto.PlaylistRequestDTO;
import com.semantyca.datanest.dto.event.EventDTO;
import com.semantyca.datanest.dto.event.EventEntryDTO;
import com.semantyca.datanest.dto.script.ScenePromptDTO;
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

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
                            .invoke(response -> LOGGER.info(
                                    "Perplexity raw JSON (brandSlug={}): {}",
                                    brand.getSlugName(),
                                    response.encodePrettily()))
                            .onItem().transformToUni(response -> persistEventsFromPerplexityArticles(response, brand, user));
                });
    }

    private static @NonNull String getQuery(CountryCode country) {
        String countryLabel = country.getCountryName();
        return "Research major public events, national holidays, elections, sports championships, "
                + "and cultural happenings in " + countryLabel + " for broadcast radio audiences. "
                + "Focus on 2025-2027 (past year + next 18 months). "
                + "For each event/article, **MANDATORILY** extract the main date as ISO yyyy-MM-dd: "
                + "- Use event's official/primary day (holidays: observance date; sports: start/opening)."
                + "- Anniversaries/festivals: pick representative date from sources (e.g., celebration day)."
                + "- Multi-day: use first day. Past: actual date. Future: scheduled."
                + "- If truly no date anywhere, use '2026-01-01' placeholder."
                + "Output schema: title, source, url, date (ALWAYS yyyy-MM-dd), content summary.";
    }

    private Uni<List<EventDTO>> persistEventsFromPerplexityArticles(JsonObject response, Brand brand, IUser user) {
        ZoneId tz = brand.getTimeZone() != null ? brand.getTimeZone() : ZoneId.of("UTC");
        String slug = brand.getSlugName() != null ? brand.getSlugName() : String.valueOf(brand.getId());
        JsonArray articles = response.getJsonArray("articles");
        List<Uni<EventDTO>> saves = new ArrayList<>();
        if (articles != null && !articles.isEmpty()) {
            int cap = Math.min(articles.size(), 25);
            for (int i = 0; i < cap; i++) {
                JsonObject article = articles.getJsonObject(i);
                if (article == null) {
                    LOGGER.warn("Perplexity article [{}] is null (brandSlug={})", i, slug);
                    continue;
                }
                LocalDate eventDay = resolveEventDay(article, response, tz);
                if (eventDay == null) {
                    LOGGER.warn(
                            "Skipping Perplexity article [{}] for brandSlug={}: no parseable yyyy-MM-dd date. raw={}",
                            i,
                            slug,
                            article.encode());
                    continue;
                }
                EventDTO dto = buildGeneratedEventDto(brand, article, eventDay);
                if (dto.getDescription() != null && !dto.getDescription().isBlank()) {
                    saves.add(upsert("new", dto, user));
                }
            }
        }
        if (saves.isEmpty()) {
            LOGGER.warn("No events saved for brandSlug={}: zero articles with a parseable yyyy-MM-dd date", slug);
            return Uni.createFrom().item(List.of());
        }
        return Uni.join().all(saves).andFailFast();
    }

    private EventDTO buildGeneratedEventDto(Brand brand, JsonObject article, LocalDate eventDay) {
        EventDTO dto = new EventDTO();
        dto.setBrandId(brand.getId().toString());
        dto.setType(EventType.SPECIAL.name());
        dto.setPriority(EventPriority.MEDIUM.name());
        ZoneId tz = brand.getTimeZone() != null ? brand.getTimeZone() : ZoneId.of("UTC");
        dto.setTimeZone(tz.getId());

        String title = article.getString("title");
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
        String body = desc.toString();
        dto.setDescription(body.isBlank() ? null : body);

        dto.setSchedule(scheduleOnceOnLocalDate(eventDay));

        PlaylistRequestDTO playlistDTO = new PlaylistRequestDTO();
        playlistDTO.setSourcing(WayOfSourcing.RANDOM.toString());
        dto.setStagePlaylist(playlistDTO);
        dto.setActions(List.of());
        return dto;
    }

    /** Strict ISO yyyy-MM-dd (word-bounded). */
    private static final Pattern ISO_LOCAL_DATE = Pattern.compile("\\b(\\d{4}-\\d{2}-\\d{2})\\b");

    /** Year-first numeric dates, e.g. 2026-5-3 or 2026/05/03 (matches your query’s ISO focus plus common variants). */
    private static final Pattern YEAR_FIRST_NUMERIC_DATE = Pattern.compile("\\b(\\d{4})[-/.](\\d{1,2})[-/.](\\d{1,2})\\b");

    private static LocalDate resolveEventDay(JsonObject article, JsonObject envelope, ZoneId tz) {
        if (article != null) {
            LocalDate d = tryParseEventDate(article.getString("date"), tz);
            if (d != null) {
                return d;
            }
            d = extractIsoLocalDateFromBlob(joinArticleFields(article));
            if (d != null) {
                return d;
            }
        }
        if (envelope != null) {
            String blob = textOrEmpty(envelope.getString("title")) + " " + textOrEmpty(envelope.getString("summary"));
            return extractIsoLocalDateFromBlob(blob);
        }
        return null;
    }

    private static String textOrEmpty(String s) {
        return s == null ? "" : s;
    }

    private static String joinArticleFields(JsonObject article) {
        StringBuilder sb = new StringBuilder();
        for (String key : List.of("title", "date", "content")) {
            String v = article.getString(key);
            if (v != null && !v.isBlank()) {
                if (sb.length() > 0) {
                    sb.append(' ');
                }
                sb.append(v.trim());
            }
        }
        return sb.toString();
    }

    private static LocalDate extractIsoLocalDateFromBlob(String text) {
        if (text == null || text.isBlank()) {
            return null;
        }
        String normalized = normalizeSeparatorsForScan(text);
        Matcher strict = ISO_LOCAL_DATE.matcher(normalized);
        if (strict.find()) {
            LocalDate d = tryParseEventDate(strict.group(1), null);
            if (d != null) {
                return d;
            }
        }
        Matcher ymd = YEAR_FIRST_NUMERIC_DATE.matcher(normalized);
        while (ymd.find()) {
            try {
                int y = Integer.parseInt(ymd.group(1));
                int mo = Integer.parseInt(ymd.group(2));
                int day = Integer.parseInt(ymd.group(3));
                return LocalDate.of(y, mo, day);
            } catch (DateTimeException | NumberFormatException ignored) {
            }
        }
        return null;
    }

    private static String normalizeSeparatorsForScan(String text) {
        return text.replace('\u2013', '-')
                .replace('\u2014', '-')
                .replace('\u2212', '-');
    }

    private static String normalizeFlexibleDateInput(String s) {
        String t = s.trim();
        t = normalizeSeparatorsForScan(t);
        t = t.replace("`", "").replace("**", "").replace("*", "").trim();
        if (t.startsWith("(") && t.endsWith(")") && t.length() > 2) {
            t = t.substring(1, t.length() - 1).trim();
        }
        return t;
    }

    private static ScheduleDTO scheduleOnceOnLocalDate(LocalDate date) {
        OnceTriggerDTO once = new OnceTriggerDTO();
        once.setStartTime("09:00");
        once.setDuration("60");
        once.setWeekdays(List.of(date.getDayOfWeek().name()));

        TaskDTO task = new TaskDTO();
        task.setId(UUID.randomUUID());
        task.setTriggerType(TriggerType.ONCE);
        task.setOnceTrigger(once);

        ScheduleDTO schedule = new ScheduleDTO();
        schedule.setEnabled(true);
        schedule.setTasks(List.of(task));
        return schedule;
    }

    /**
     * Best-effort parse of a model-provided date string into a calendar day in {@code tz} when relevant.
     */
    private static LocalDate tryParseEventDate(String raw, ZoneId tz) {
        if (raw == null) {
            return null;
        }
        String s = normalizeFlexibleDateInput(raw);
        if (s.isEmpty()) {
            return null;
        }
        String lower = s.toLowerCase(Locale.ROOT);
        if (lower.contains("tbd") || lower.contains("tbc") || "n/a".equals(lower) || "unknown".equals(lower)) {
            return null;
        }
        int paren = s.indexOf('(');
        if (paren > 0) {
            s = s.substring(0, paren).trim();
        }
        int t = s.indexOf('T');
        if (t > 0) {
            s = s.substring(0, t).trim();
        }
        int space = s.indexOf(' ');
        if (space > 0 && s.chars().filter(ch -> ch == '-').count() >= 2) {
            s = s.substring(0, space).trim();
        }

        try {
            return LocalDate.parse(s);
        } catch (DateTimeParseException ignored) {
        }
        ZoneId zone = tz != null ? tz : ZoneId.of("UTC");
        try {
            return ZonedDateTime.parse(s).withZoneSameInstant(zone).toLocalDate();
        } catch (DateTimeParseException ignored) {
        }
        List<DateTimeFormatter> formatters = List.of(
                DateTimeFormatter.ofPattern("MMMM d, yyyy", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("d MMMM yyyy", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("MMM d, yyyy", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("d MMM yyyy", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("M/d/yyyy", Locale.ENGLISH),
                DateTimeFormatter.ofPattern("d/M/yyyy", Locale.ENGLISH)
        );
        for (DateTimeFormatter formatter : formatters) {
            try {
                return LocalDate.parse(s, formatter);
            } catch (DateTimeParseException ignored) {
            }
        }
        return null;
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
                    dto.setMandatory(action.isMandatory());
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
                    scenePrompt.setMandatory(dto.isMandatory());
                    return scenePrompt;
                })
                .collect(Collectors.toList());
    }

    private PlaylistRequestDTO mapStagePlaylistToDTO(PlaylistRequest playlistRequest) {
        if (playlistRequest == null) {
            return null;
        }
        PlaylistRequestDTO dto = new PlaylistRequestDTO();
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

    private PlaylistRequest mapDTOToStagePlaylist(PlaylistRequestDTO dto) {
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
