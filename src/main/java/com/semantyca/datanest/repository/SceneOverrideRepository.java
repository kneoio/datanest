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
import com.semantyca.mixpla.model.PlaylistRequest;
import com.semantyca.mixpla.model.SceneOverride;
import com.semantyca.mixpla.repository.MixplaNameResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.semantyca.mixpla.repository.MixplaNameResolver.SCENE_OVERRIDE;

@ApplicationScoped
public class SceneOverrideRepository extends AsyncRepository {
    private static final EntityData entityData = MixplaNameResolver.create().getEntityNames(SCENE_OVERRIDE);

    @Inject
    public SceneOverrideRepository(Pool client, ObjectMapper mapper, RLSRepository rlsRepository) {
        super(client, mapper, rlsRepository);
    }

    public Uni<List<SceneOverride>> getAll(int limit, int offset, boolean includeArchived, IUser user) {
        String sql = "SELECT t.* FROM " + entityData.getTableName() + " t, " + entityData.getRlsName() + " rls " +
                "WHERE t.id = rls.entity_id AND rls.reader = $1";
        if (!includeArchived) {
            sql += " AND t.archived = 0";
        }
        sql += " ORDER BY t.reg_date DESC";
        if (limit > 0) {
            sql += String.format(" LIMIT %s OFFSET %s", limit, offset);
        }
        return client.preparedQuery(sql)
                .execute(Tuple.of(user.getId()))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    public Uni<Integer> getAllCount(IUser user, boolean includeArchived) {
        String sql = "SELECT COUNT(*) FROM " + entityData.getTableName() + " t, " + entityData.getRlsName() + " rls " +
                "WHERE t.id = rls.entity_id AND rls.reader = $1";
        if (!includeArchived) {
            sql += " AND t.archived = 0";
        }
        return client.preparedQuery(sql)
                .execute(Tuple.of(user.getId()))
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<SceneOverride> findById(UUID id, IUser user, boolean includeArchived) {
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
                });
    }

    public Uni<SceneOverride> insert(SceneOverride entity, List<RlsActionDTO> rlsActions, IUser user) {
        OffsetDateTime nowTime = OffsetDateTime.now();
        String sql = "INSERT INTO " + entityData.getTableName() +
                " (author, reg_date, last_mod_user, last_mod_date, title, scene_id, start_time, actions_data, stage_playlist, weekdays) " +
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id";
        Tuple params = Tuple.tuple()
                .addLong(user.getId())
                .addOffsetDateTime(nowTime)
                .addLong(user.getId())
                .addOffsetDateTime(nowTime)
                .addString(entity.getTitle())
                .addUUID(entity.getSceneId())
                .addJsonArray(entity.getStartTime() != null ? new JsonArray(entity.getStartTime()) : new JsonArray())
                .addJsonArray(entity.getActionsData() != null ? new JsonArray(entity.getActionsData()) : new JsonArray())
                .addJsonObject(entity.getStagePlaylist() != null ? JsonObject.mapFrom(entity.getStagePlaylist()) : null)
                .addArrayOfInteger(entity.getWeekdays() != null ? entity.getWeekdays().toArray(new Integer[0]) : null);
        return client.withTransaction(tx ->
                tx.preparedQuery(sql)
                        .execute(params)
                        .onItem().transform(result -> result.iterator().next().getUUID("id"))
                        .onItem().transformToUni(id ->
                                insertRLSPermissions(tx, id, entityData, user)
                                        .onItem().transformToUni(ignored -> applyRlsActions(tx, id, rlsActions))
                                        .onItem().transform(ignored -> id)
                        )
        ).onItem().transformToUni(id -> findById(id, user, true));
    }

    public Uni<SceneOverride> update(UUID id, SceneOverride entity, List<RlsActionDTO> rlsActions, IUser user) {
        return rlsRepository.findById(entityData.getRlsName(), user.getId(), id)
                .onItem().transformToUni(permissions -> {
                    if (!permissions[0]) {
                        return Uni.createFrom().failure(new DocumentModificationAccessException(
                                "User does not have edit permission", user.getUserName(), id));
                    }
                    OffsetDateTime nowTime = OffsetDateTime.now();
                    String sql = "UPDATE " + entityData.getTableName() +
                            " SET title=$1, scene_id=$2, start_time=$3, actions_data=$4, stage_playlist=$5, weekdays=$6, last_mod_user=$7, last_mod_date=$8 WHERE id=$9";
                    Tuple params = Tuple.tuple()
                            .addString(entity.getTitle())
                            .addUUID(entity.getSceneId())
                            .addJsonArray(entity.getStartTime() != null ? new JsonArray(entity.getStartTime()) : new JsonArray())
                            .addJsonArray(entity.getActionsData() != null ? new JsonArray(entity.getActionsData()) : new JsonArray())
                            .addJsonObject(entity.getStagePlaylist() != null ? JsonObject.mapFrom(entity.getStagePlaylist()) : null)
                            .addArrayOfInteger(entity.getWeekdays() != null ? entity.getWeekdays().toArray(new Integer[0]) : null)
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
                                        return applyRlsActions(tx, id, rlsActions);
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
                        return Uni.createFrom().failure(new DocumentModificationAccessException(
                                "User does not have delete permission", user.getUserName(), id));
                    }
                    return client.withTransaction(tx -> {
                        String deleteRlsSql = String.format("DELETE FROM %s WHERE entity_id = $1", entityData.getRlsName());
                        String deleteEntitySql = String.format("DELETE FROM %s WHERE id = $1", entityData.getTableName());
                        return tx.preparedQuery(deleteRlsSql).execute(Tuple.of(id))
                                .onItem().transformToUni(ignored -> tx.preparedQuery(deleteEntitySql).execute(Tuple.of(id)))
                                .onItem().transform(RowSet::rowCount);
                    });
                });
    }

    private SceneOverride from(Row row) {
        SceneOverride doc = new SceneOverride();
        setDefaultFields(doc, row);
        doc.setTitle(row.getString("title"));
        doc.setSceneId(row.getUUID("scene_id"));
        doc.setArchived(row.getInteger("archived"));
        JsonArray startTimeJson = row.getJsonArray("start_time");
        if (startTimeJson != null && !startTimeJson.isEmpty()) {
            try {
                List<LocalTime> startTimes = mapper.readValue(startTimeJson.encode(),
                        new com.fasterxml.jackson.core.type.TypeReference<List<LocalTime>>() {});
                doc.setStartTime(startTimes);
            } catch (Exception e) {
                doc.setStartTime(List.of());
            }
        } else {
            doc.setStartTime(List.of());
        }
        JsonArray actionsJson = row.getJsonArray("actions_data");
        doc.setActionsData(actionsJson != null ? actionsJson.getList() : List.of());
        JsonObject stagePlaylistJson = row.getJsonObject("stage_playlist");
        if (stagePlaylistJson != null) {
            try {
                doc.setStagePlaylist(mapper.convertValue(stagePlaylistJson.getMap(), PlaylistRequest.class));
            } catch (Exception e) {
                LOGGER.error("Failed to parse stage_playlist JSON for scene override: {}", row.getUUID("id"), e);
            }
        }
        Object[] weekdaysArr = row.getArrayOfIntegers("weekdays");
        if (weekdaysArr != null && weekdaysArr.length > 0) {
            List<Integer> weekdays = new ArrayList<>();
            for (Object o : weekdaysArr) {
                weekdays.add((Integer) o);
            }
            doc.setWeekdays(weekdays);
        }
        return doc;
    }

    public Uni<List<DocumentAccessInfo>> getDocumentAccessInfo(UUID documentId, IUser user) {
        return getDocumentAccessInfo(documentId, entityData, user);
    }
}
