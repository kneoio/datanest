package com.semantyca.datanest.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.embedded.DocumentAccessInfo;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.exception.DocumentModificationAccessException;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.core.repository.rls.RlsActionUtil;
import com.semantyca.core.repository.table.EntityData;
import com.semantyca.datanest.repository.prompt.PromptRepository;
import com.semantyca.mixpla.model.CustomAction;
import com.semantyca.mixpla.model.PlaylistRequest;
import com.semantyca.mixpla.model.Scene;
import com.semantyca.mixpla.model.filter.SceneFilter;
import com.semantyca.mixpla.repository.MixplaNameResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.SqlClient;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.semantyca.mixpla.repository.MixplaNameResolver.SCRIPT_SCENE;

@ApplicationScoped
public class SceneRepository extends AsyncRepository {
    private static final EntityData entityData = MixplaNameResolver.create().getEntityNames(SCRIPT_SCENE);
    /** 6am as start of "broadcast day" for ordering (times before 6am sort after evening). */
    private static final int BROADCAST_DAY_ANCHOR_SEC = 6 * 60 * 60;
    private final PromptRepository promptRepository;

    /**
     * SQL expression: sort position from 6am (first slot in {@code start_time} JSON array); empty/null last.
     */
    private static String broadcastDayStartTimeSortKey(String tableAlias) {
        return "CASE WHEN (" + tableAlias + ".start_time->>0) IS NOT NULL "
                + "THEN MOD((EXTRACT(EPOCH FROM (" + tableAlias + ".start_time->>0)::time) - " + BROADCAST_DAY_ANCHOR_SEC
                + " + 86400)::numeric, 86400) "
                + "ELSE NULL END";
    }

    @Inject
    public SceneRepository(Pool client, ObjectMapper mapper, RLSRepository rlsRepository, PromptRepository promptRepository) {
        super(client, mapper, rlsRepository);
        this.promptRepository = promptRepository;
    }

    public Uni<List<Scene>> getAll(int limit, int offset, boolean includeArchived, IUser user, SceneFilter filter) {
        String sql = "SELECT t.*, s.name as script_title FROM " + entityData.getTableName() + " t, " + entityData.getRlsName() + " rls, mixpla__scripts s " +
                "WHERE t.id = rls.entity_id AND t.script_id = s.id AND rls.reader = $1";
        if (!includeArchived) {
            sql += " AND t.archived = 0";
        }
        if (filter != null && filter.isActivated() && filter.getTimingMode() != null) {
            sql += " AND s.timing_mode = '" + filter.getTimingMode().name() + "'";
        }
        if (filter != null && filter.isActivated() && filter.getScriptId() != null) {
            sql += " AND t.script_id = '" + filter.getScriptId() + "'";
        }
        if (filter != null && filter.getCustomScript() != null) {
            sql += " AND s.custom = " + filter.getCustomScript();
        }
        sql += " ORDER BY t.script_id ASC, " + broadcastDayStartTimeSortKey("t") + " ASC NULLS LAST, t.seq_num ASC";
        if (limit > 0) {
            sql += String.format(" LIMIT %s OFFSET %s", limit, offset);
        }
        return client.preparedQuery(sql)
                .execute(Tuple.of(user.getId()))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::fromViewEntry)
                .collect().asList();
    }

    public Uni<Integer> getAllCount(IUser user, boolean includeArchived, SceneFilter filter) {
        String sql = "SELECT COUNT(*) FROM " + entityData.getTableName() + " t, " + entityData.getRlsName() + " rls, mixpla__scripts s " +
                "WHERE t.id = rls.entity_id AND t.script_id = s.id AND rls.reader = $1";
        if (!includeArchived) {
            sql += " AND t.archived = 0";
        }
        if (filter != null && filter.isActivated() && filter.getTimingMode() != null) {
            sql += " AND s.timing_mode = '" + filter.getTimingMode().name() + "'";
        }
        if (filter != null && filter.isActivated() && filter.getScriptId() != null) {
            sql += " AND t.script_id = '" + filter.getScriptId() + "'";
        }
        if (filter != null && filter.getCustomScript() != null) {
            sql += " AND s.custom = " + filter.getCustomScript();
        }
        return client.preparedQuery(sql)
                .execute(Tuple.of(user.getId()))
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    // Per-script listing
    public Uni<List<Scene>> listByScript(UUID scriptId, int limit, int offset, boolean includeArchived, IUser user) {
        String sql = "SELECT t.* FROM " + entityData.getTableName() + " t, " + entityData.getRlsName() + " rls " +
                "WHERE t.id = rls.entity_id AND rls.reader = $1 AND t.script_id = $2";
        if (!includeArchived) {
            sql += " AND t.archived = 0";
        }
        sql += " ORDER BY t.seq_num ASC, " + broadcastDayStartTimeSortKey("t") + " ASC NULLS LAST";
        if (limit > 0) {
            sql += String.format(" LIMIT %s OFFSET %s", limit, offset);
        }
        return client.preparedQuery(sql)
                .execute(Tuple.of(user.getId(), scriptId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList()
                .onItem().transformToUni(scenes -> {
                    if (scenes.isEmpty()) {
                        return Uni.createFrom().item(scenes);
                    }
                    List<Uni<Scene>> sceneUnis = scenes.stream()
                            .map(scene -> promptRepository.getPromptsForScene(scene.getId())
                                    .onItem().transform(promptIds -> {
                                        scene.setIntroPrompts(promptIds);
                                        return scene;
                                    }))
                            .collect(java.util.stream.Collectors.toList());
                    return Uni.join().all(sceneUnis).andFailFast();
                });
    }

    public Uni<Integer> countByScript(UUID scriptId, boolean includeArchived, IUser user) {
        String sql = "SELECT COUNT(*) FROM " + entityData.getTableName() + " t, " + entityData.getRlsName() + " rls " +
                "WHERE t.id = rls.entity_id AND rls.reader = $1 AND t.script_id = $2";
        if (!includeArchived) {
            sql += " AND t.archived = 0";
        }
        return client.preparedQuery(sql)
                .execute(Tuple.of(user.getId(), scriptId))
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<Scene> findById(UUID id, IUser user, boolean includeArchived) {
        String sql = "SELECT theTable.*, rls.* FROM %s theTable JOIN %s rls ON theTable.id = rls.entity_id WHERE rls.reader = $1 AND theTable.id = $2";
        if (!includeArchived) {
            sql += " AND theTable.archived = 0";
        }
        return client.preparedQuery(String.format(sql, entityData.getTableName(), entityData.getRlsName()))
                .execute(Tuple.of(user.getId(), id))
                .onItem().transform(RowSet::iterator)
                .onItem().transform(iterator -> {
                    if (iterator.hasNext()) {
                        return from(iterator.next());
                    } else {
                        throw new DocumentHasNotFoundException(id);
                    }
                })
                .onItem().transformToUni(scene ->
                        promptRepository.getPromptsForScene(id)
                                .onItem().transform(promptIds -> {
                                    scene.setIntroPrompts(promptIds);
                                    return scene;
                                })
                );
    }

    public Uni<Scene> insert(Scene scene, IUser user) {
        return insert(scene, List.of(), user);
    }

    public Uni<Scene> insert(Scene scene, List<RlsActionDTO> rlsActions, IUser user) {
        OffsetDateTime nowTime = OffsetDateTime.now();
        String sql = "INSERT INTO " + entityData.getTableName() +
                " (author, reg_date, last_mod_user, last_mod_date, script_id, title, start_time, duration_seconds, seq_num, one_time_run, allow_jingles, allow_ads, talkativity, weekdays, stage_playlist, actions) " +
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING id";
        Tuple params = Tuple.tuple()
                .addLong(user.getId())
                .addOffsetDateTime(nowTime)
                .addLong(user.getId())
                .addOffsetDateTime(nowTime)
                .addUUID(scene.getScriptId())
                .addString(scene.getTitle())
                .addJsonArray(scene.getStartTime() != null ? new JsonArray(scene.getStartTime()) : new JsonArray())
                .addInteger(scene.getDurationSeconds())
                .addInteger(scene.getSeqNum())
                .addBoolean(scene.isOneTimeRun())
                .addBoolean(scene.isAllowJingles())
                .addBoolean(scene.isAllowAds())
                .addDouble(scene.getTalkativity())
                .addArrayOfInteger(scene.getWeekdays() != null ? scene.getWeekdays().toArray(new Integer[0]) : null)
                .addJsonObject(scene.getPlaylistRequest() != null ? JsonObject.mapFrom(scene.getPlaylistRequest()) : null)
                .addJsonArray(scene.getActions() != null && !scene.getActions().isEmpty() ? new JsonArray(scene.getActions().stream().map(JsonObject::mapFrom).toList()) : new JsonArray());
        return client.withTransaction(tx ->
                tx.preparedQuery(sql)
                        .execute(params)
                        .onItem().transform(result -> result.iterator().next().getUUID("id"))
                        .onItem().transformToUni(id ->
                                insertRLSPermissions(tx, id, entityData, user)
                                        .onItem().transformToUni(ignored -> promptRepository.updatePromptsForScene(tx, id, scene.getIntroPrompts()))
                                        .onItem().transformToUni(ignored -> applyRlsActions(tx, id, rlsActions))
                                        .onItem().transform(ignored -> id)
                        )
        ).onItem().transformToUni(id -> findById(id, user, true));
    }

    public Uni<Scene> update(UUID id, Scene scene, List<RlsActionDTO> rlsActions, IUser user) {
        return rlsRepository.findById(entityData.getRlsName(), user.getId(), id)
                .onItem().transformToUni(permissions -> {
                    if (!permissions[0]) {
                        return Uni.createFrom().failure(new DocumentModificationAccessException("User does not have edit permission", user.getUserName(), id));
                    }
                    OffsetDateTime nowTime = OffsetDateTime.now();
                    String sql = "UPDATE " + entityData.getTableName() +
                            " SET title=$1, start_time=$2, duration_seconds=$3, seq_num=$4, one_time_run=$5, allow_jingles=$6, allow_ads=$7, talkativity=$8, weekdays=$9, stage_playlist=$10, actions=$11, last_mod_user=$12, last_mod_date=$13 WHERE id=$14";
                    Tuple params = Tuple.tuple()
                            .addString(scene.getTitle())
                            .addJsonArray(scene.getStartTime() != null ? new JsonArray(scene.getStartTime()) : new JsonArray())
                            .addInteger(scene.getDurationSeconds())
                            .addInteger(scene.getSeqNum())
                            .addBoolean(scene.isOneTimeRun())
                            .addBoolean(scene.isAllowJingles())
                            .addBoolean(scene.isAllowAds())
                            .addDouble(scene.getTalkativity())
                            .addArrayOfInteger(scene.getWeekdays() != null ? scene.getWeekdays().toArray(new Integer[0]) : null)
                            .addJsonObject(scene.getPlaylistRequest() != null ? JsonObject.mapFrom(scene.getPlaylistRequest()) : null)
                            .addValue(scene.getActions() != null && !scene.getActions().isEmpty() ? new JsonArray(scene.getActions().stream().map(JsonObject::mapFrom).toList()) : new JsonArray())
                            .addLong(user.getId())
                            .addOffsetDateTime(nowTime)
                            .addUUID(id);
                    return client.withTransaction(tx ->
                            tx.preparedQuery(sql)
                                    .execute(params)
                                    .onItem().transformToUni(rowSet -> {
                                        if (rowSet.rowCount() == 0) {
                                            return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                                        }
                                        return promptRepository.updatePromptsForScene(tx, id, scene.getIntroPrompts())
                                                .onItem().transformToUni(ignored -> applyRlsActions(tx, id, rlsActions));
                                    })
                    ).onItem().transformToUni(ignored -> findById(id, user, true));
                });
    }

    public Uni<Void> bulkUpdateACL(UUID entityId, List<RlsActionDTO> actions, IUser user) {
        return rlsRepository.findById(entityData.getRlsName(), user.getId(), entityId)
                .onItem().transformToUni(permissions -> {
                    if (!permissions[0]) {
                        return Uni.createFrom().failure(new DocumentModificationAccessException(
                                "User does not have edit permission", user.getUserName(), entityId));
                    }
                    return client.withTransaction(tx -> applyRlsActions(tx, entityId, actions));
                });
    }

    private Uni<Void> applyRlsActions(SqlClient tx, UUID entityId, List<RlsActionDTO> actions) {
        return RlsActionUtil.applyRlsActions(tx, entityData.getRlsName(), entityId, actions);
    }

    public Uni<Integer> archive(UUID id, IUser user) {
        return archive(id, entityData, user);
    }

    public Uni<Integer> delete(UUID id, IUser user) {
        return rlsRepository.findById(entityData.getRlsName(), user.getId(), id)
                .onItem().transformToUni(permissions -> {
                    if (!permissions[1]) {
                        return Uni.createFrom().failure(new DocumentModificationAccessException("User does not have delete permission", user.getUserName(), id));
                    }
                    return client.withTransaction(tx -> {
                        String deletePromptsSql = "DELETE FROM mixpla__script_scene_prompts WHERE script_scene_id = $1";
                        String deleteRlsSql = String.format("DELETE FROM %s WHERE entity_id = $1", entityData.getRlsName());
                        String deleteEntitySql = String.format("DELETE FROM %s WHERE id = $1", entityData.getTableName());
                        return tx.preparedQuery(deletePromptsSql).execute(Tuple.of(id))
                                .onItem().transformToUni(ignored -> tx.preparedQuery(deleteRlsSql).execute(Tuple.of(id)))
                                .onItem().transformToUni(ignored -> tx.preparedQuery(deleteEntitySql).execute(Tuple.of(id)))
                                .onItem().transform(RowSet::rowCount);
                    });
                });
    }

    private Scene fromViewEntry(Row row) {
        Scene doc = from(row);
        String scriptTitle = row.getString("script_title");
        if (scriptTitle != null) {
            doc.setScriptTitle(scriptTitle);
        }
        return doc;
    }

    private Scene from(Row row) {
        Scene doc = new Scene();
        setDefaultFields(doc, row);
        doc.setScriptId(row.getUUID("script_id"));
        doc.setTitle(row.getString("title"));
        doc.setArchived(row.getInteger("archived"));
        JsonArray startTimeJson = row.getJsonArray("start_time");
        if (startTimeJson != null && !startTimeJson.isEmpty()) {
            try {
                List<LocalTime> startTimes = mapper.readValue(startTimeJson.encode(), new com.fasterxml.jackson.core.type.TypeReference<List<LocalTime>>() {});
                doc.setStartTime(startTimes);
            } catch (Exception e) {
                doc.setStartTime(List.of());
            }
        } else {
            doc.setStartTime(List.of());
        }
        Integer durationSeconds = row.getInteger("duration_seconds");
        if (durationSeconds != null) doc.setDurationSeconds(durationSeconds);
        Integer seqNum = row.getInteger("seq_num");
        if (seqNum != null) doc.setSeqNum(seqNum);
        Boolean oneTimeRun = row.getBoolean("one_time_run");
        if (oneTimeRun != null) doc.setOneTimeRun(oneTimeRun);
        Boolean allowJingles = row.getBoolean("allow_jingles");
        if (allowJingles != null) doc.setAllowJingles(allowJingles);
        Boolean allowAds = row.getBoolean("allow_ads");
        if (allowAds != null) doc.setAllowAds(allowAds);
        Double talk = row.getDouble("talkativity");
        if (talk != null) doc.setTalkativity(talk);
        Object[] weekdaysArr = row.getArrayOfIntegers("weekdays");
        if (weekdaysArr != null && weekdaysArr.length > 0) {
            List<Integer> weekdays = new ArrayList<>();
            for (Object o : weekdaysArr) {
                weekdays.add((Integer) o);
            }
            doc.setWeekdays(weekdays);
        }
        JsonObject stagePlaylistJson = row.getJsonObject("stage_playlist");
        if (stagePlaylistJson != null) {
            try {
                PlaylistRequest playlistRequest = mapper.convertValue(stagePlaylistJson.getMap(), PlaylistRequest.class);
                doc.setPlaylistRequest(playlistRequest);
            } catch (Exception e) {
                LOGGER.error("Failed to parse stage_playlist JSON for scene: {}", row.getUUID("id"), e);
            }
        }
        JsonArray actionsJson = row.getJsonArray("actions");
        if (actionsJson != null && !actionsJson.isEmpty()) {
            try {
                doc.setActions(mapper.readValue(actionsJson.encode(), new com.fasterxml.jackson.core.type.TypeReference<List<CustomAction>>() {}));
            } catch (Exception e) {
                LOGGER.error("Failed to parse actions JSON for scene: {}", row.getUUID("id"), e);
            }
        }
        return doc;
    }

    public Uni<List<DocumentAccessInfo>> getDocumentAccessInfo(UUID documentId, IUser user) {
        return getDocumentAccessInfo(documentId, entityData, user);
    }
}
