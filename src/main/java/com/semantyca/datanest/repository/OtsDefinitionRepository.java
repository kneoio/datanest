package com.semantyca.datanest.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
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
import com.semantyca.mixpla.model.cnst.OtsRunStatus;
import com.semantyca.mixpla.model.cnst.OtsRunType;
import com.semantyca.mixpla.model.filter.OtsDefinitionFilter;
import com.semantyca.mixpla.model.stream.OtsDefinition;
import com.semantyca.mixpla.model.stream.OtsStatusHistoryEntry;
import com.semantyca.mixpla.repository.MixplaNameResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.semantyca.mixpla.repository.MixplaNameResolver.OTS_DEFINITION;

@ApplicationScoped
public class OtsDefinitionRepository extends AsyncRepository {
    private static final Logger LOGGER = Logger.getLogger(OtsDefinitionRepository.class);
    private static final EntityData entityData = MixplaNameResolver.create().getEntityNames(OTS_DEFINITION);

    @Inject
    public OtsDefinitionRepository(Pool client, ObjectMapper mapper, RLSRepository rlsRepository) {
        super(client, mapper, rlsRepository);
    }

    public Uni<List<OtsDefinition>> getAll(int limit, int offset, boolean includeArchived, final IUser user, final OtsDefinitionFilter filter) {
        String sql = """
                    SELECT t.*
                    FROM %s t
                    JOIN %s rls ON t.id = rls.entity_id
                    WHERE rls.reader = %s
                """.formatted(entityData.getTableName(), entityData.getRlsName(), user.getId());

        if (!includeArchived) {
            sql += " AND t.archived = 0";
        }

        if (filter != null && filter.isActivated()) {
            sql += buildFilterConditions(filter);
        }

        sql += " ORDER BY t.last_mod_date DESC";

        if (limit > 0) {
            sql += String.format(" LIMIT %s OFFSET %s", limit, offset);
        }

        if (filter != null && filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty()) {
            return client.preparedQuery(sql)
                    .execute(Tuple.of(filter.getSearchTerm().trim()))
                    .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                    .onItem().transform(this::from)
                    .collect().asList();
        }

        return client.query(sql)
                .execute()
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    public Uni<Integer> getAllCount(IUser user, boolean includeArchived, OtsDefinitionFilter filter) {
        String sql = "SELECT COUNT(*) FROM " + entityData.getTableName() + " t, " + entityData.getRlsName() + " rls " +
                "WHERE t.id = rls.entity_id AND rls.reader = " + user.getId();

        if (!includeArchived) {
            sql += " AND t.archived = 0";
        }

        if (filter != null && filter.isActivated()) {
            sql += buildFilterConditions(filter);
        }

        if (filter != null && filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty()) {
            return client.preparedQuery(sql)
                    .execute(Tuple.of(filter.getSearchTerm().trim()))
                    .onItem().transform(rows -> rows.iterator().next().getInteger(0));
        }

        return client.query(sql)
                .execute()
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    private String buildFilterConditions(OtsDefinitionFilter filter) {
        StringBuilder conditions = new StringBuilder();

        if (filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty()) {
            conditions.append(" AND t.name ILIKE '%' || $1 || '%'");
        }

        if (filter.getBrandId() != null) {
            conditions.append(" AND t.brand_id = '").append(filter.getBrandId()).append("'");
        }

        return conditions.toString();
    }

    public Uni<Boolean> existsBySlug(String slugName) {
        String sql = "SELECT 1 FROM " + entityData.getTableName() + " WHERE slug_name = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(slugName))
                .onItem().transform(rows -> rows.iterator().hasNext());
    }

    public Uni<OtsDefinition> findById(UUID id, IUser user, boolean includeArchived) {
        String sql = "SELECT theTable.*, rls.* " +
                "FROM %s theTable " +
                "JOIN %s rls ON theTable.id = rls.entity_id " +
                "WHERE rls.reader = $1 AND theTable.id = $2";

        if (!includeArchived) {
            sql += " AND (theTable.archived IS NULL OR theTable.archived = 0)";
        }

        return client.preparedQuery(String.format(sql, entityData.getTableName(), entityData.getRlsName()))
                .execute(Tuple.of(user.getId(), id))
                .onItem().transform(RowSet::iterator)
                .onItem().transformToUni(iterator -> {
                    if (iterator.hasNext()) {
                        return Uni.createFrom().item(from(iterator.next()));
                    } else {
                        LOGGER.warnf("No %s found with id: %s, user: %s ", OTS_DEFINITION, id, user.getId());
                        return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                    }
                });
    }

    public Uni<OtsDefinition> insert(OtsDefinition ots, IUser user) {
        return insert(ots, List.of(), user);
    }

    public Uni<OtsDefinition> insert(OtsDefinition ots, List<RlsActionDTO> rlsActions, IUser user) {
        return Uni.createFrom().deferred(() -> {
            try {
                String sql = "INSERT INTO " + entityData.getTableName() +
                        " (author, reg_date, last_mod_user, last_mod_date, name, slug_name, script_id, user_variables, brand_id, agent_id, status, status_history, type, estimated_duration_min, chat_context) " +
                        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING id";

                OffsetDateTime now = OffsetDateTime.now();

                JsonObject userVarsJson = null;
                if (ots.getUserVariables() != null && !ots.getUserVariables().isEmpty()) {
                    userVarsJson = new JsonObject(mapper.writeValueAsString(ots.getUserVariables()));
                }

                OtsRunStatus status = ots.getStatus() != null ? ots.getStatus() : OtsRunStatus.PENDING;
                OtsRunType type = ots.getType() != null ? ots.getType() : OtsRunType.ONE_SHOT;

                OtsStatusHistoryEntry initialEntry = new OtsStatusHistoryEntry();
                initialEntry.setStatus(status);
                initialEntry.setTimestamp(now.toZonedDateTime());
                JsonArray statusHistoryJson = new JsonArray(List.of(JsonObject.mapFrom(initialEntry)));

                Tuple params = Tuple.tuple()
                        .addLong(user.getId())
                        .addOffsetDateTime(now)
                        .addLong(user.getId())
                        .addOffsetDateTime(now)
                        .addString(ots.getName())
                        .addString(ots.getSlugName())
                        .addUUID(ots.getScriptId())
                        .addJsonObject(userVarsJson)
                        .addUUID(ots.getBrandId())
                        .addUUID(ots.getAgentId())
                        .addString(status.name())
                        .addJsonArray(statusHistoryJson)
                        .addString(type.name())
                        .addInteger(ots.getEstimatedDurationMin())
                        .addString(ots.getChatContext());

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
            } catch (Exception e) {
                LOGGER.errorf("Failed to insert ots definition for user: %s", user.getId(), e);
                return Uni.createFrom().failure(e);
            }
        });
    }

    public Uni<OtsDefinition> update(UUID id, OtsDefinition ots, IUser user) {
        return update(id, ots, List.of(), user);
    }

    public Uni<OtsDefinition> update(UUID id, OtsDefinition ots, List<RlsActionDTO> rlsActions, IUser user) {
        return Uni.createFrom().deferred(() -> {
            try {
                JsonObject userVarsJson = null;
                if (ots.getUserVariables() != null && !ots.getUserVariables().isEmpty()) {
                    userVarsJson = new JsonObject(mapper.writeValueAsString(ots.getUserVariables()));
                }
                JsonObject finalUserVarsJson = userVarsJson;

                return rlsRepository.findById(entityData.getRlsName(), user.getId(), id)
                        .onFailure().invoke(throwable -> LOGGER.errorf("Failed to check RLS permissions for update ots definition: %s by user: %s", id, user.getId(), throwable))
                        .onItem().transformToUni(permissions -> {
                            if (!permissions[0]) {
                                return Uni.createFrom().failure(
                                        new DocumentModificationAccessException("User does not have edit permission", user.getUserName(), id)
                                );
                            }

                            String sql = "UPDATE " + entityData.getTableName() +
                                    " SET name=$1, script_id=$2, user_variables=$3, brand_id=$4, agent_id=$5, type=$6, estimated_duration_min=$7, chat_context=$8, last_mod_user=$9, last_mod_date=$10 " +
                                    "WHERE id=$11";

                            OffsetDateTime now = OffsetDateTime.now();
                            OtsRunType type = ots.getType() != null ? ots.getType() : OtsRunType.ONE_SHOT;

                            Tuple params = Tuple.tuple()
                                    .addString(ots.getName())
                                    .addUUID(ots.getScriptId())
                                    .addJsonObject(finalUserVarsJson)
                                    .addUUID(ots.getBrandId())
                                    .addUUID(ots.getAgentId())
                                    .addString(type.name())
                                    .addInteger(ots.getEstimatedDurationMin())
                                    .addString(ots.getChatContext())
                                    .addLong(user.getId())
                                    .addOffsetDateTime(now)
                                    .addUUID(id);

                            return client.preparedQuery(sql)
                                    .execute(params)
                                    .onFailure().invoke(throwable -> LOGGER.errorf("Failed to update ots definition: %s by user: %s", id, user.getId(), throwable))
                                    .onItem().transformToUni(rowSet -> {
                                        if (rowSet.rowCount() == 0) {
                                            return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                                        }
                                        return applyRlsActions(client, id, rlsActions)
                                                .onItem().transformToUni(ignored -> findById(id, user, true));
                                    });
                        });
            } catch (Exception e) {
                LOGGER.errorf("Failed to prepare update parameters for ots definition: %s by user: %s", id, user.getId(), e);
                return Uni.createFrom().failure(e);
            }
        });
    }

    public Uni<OtsDefinition> updateStatus(UUID id, OtsRunStatus status, IUser user) {
        OtsStatusHistoryEntry entry = new OtsStatusHistoryEntry();
        entry.setStatus(status);
        entry.setTimestamp(ZonedDateTime.now());

        String sql = "UPDATE " + entityData.getTableName() +
                " SET status=$1, status_history = status_history || $2::jsonb, last_mod_user=$3, last_mod_date=$4 " +
                "WHERE id=$5";

        Tuple params = Tuple.tuple()
                .addString(status.name())
                .addJsonArray(new JsonArray(List.of(JsonObject.mapFrom(entry))))
                .addLong(user.getId())
                .addOffsetDateTime(OffsetDateTime.now())
                .addUUID(id);

        return client.preparedQuery(sql)
                .execute(params)
                .onItem().transformToUni(rowSet -> {
                    if (rowSet.rowCount() == 0) {
                        return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                    }
                    return findById(id, user, true);
                });
    }

    public Uni<Integer> archive(UUID id, IUser user) {
        return archive(id, entityData, user);
    }

    public Uni<Integer> delete(UUID id, IUser user) {
        return delete(id, entityData, user);
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

    public Uni<List<DocumentAccessInfo>> getDocumentAccessInfo(UUID documentId, IUser user) {
        return getDocumentAccessInfo(documentId, entityData, user);
    }

    private OtsDefinition from(Row row) {
        OtsDefinition doc = new OtsDefinition();
        setDefaultFields(doc, row);
        doc.setName(row.getString("name"));
        doc.setSlugName(row.getString("slug_name"));
        doc.setScriptId(row.getUUID("script_id"));
        doc.setBrandId(row.getUUID("brand_id"));
        doc.setAgentId(row.getUUID("agent_id"));

        String statusValue = row.getString("status");
        if (statusValue != null) {
            doc.setStatus(OtsRunStatus.valueOf(statusValue));
        }

        String typeValue = row.getString("type");
        if (typeValue != null) {
            doc.setType(OtsRunType.valueOf(typeValue));
        }

        doc.setEstimatedDurationMin(row.getInteger("estimated_duration_min"));
        doc.setChatContext(row.getString("chat_context"));

        JsonArray statusHistoryJson = row.getJsonArray("status_history");
        if (statusHistoryJson != null && !statusHistoryJson.isEmpty()) {
            try {
                List<OtsStatusHistoryEntry> history = mapper.readValue(statusHistoryJson.encode(), new TypeReference<>() {
                });
                doc.setStatusHistory(history);
            } catch (JsonProcessingException e) {
                doc.setStatusHistory(null);
            }
        }

        JsonObject userVarsJson = row.getJsonObject("user_variables");
        if (userVarsJson != null && !userVarsJson.isEmpty()) {
            try {
                Map<String, Object> userVars = mapper.readValue(userVarsJson.encode(), new TypeReference<>() {
                });
                doc.setUserVariables(userVars);
            } catch (JsonProcessingException e) {
                doc.setUserVariables(null);
            }
        }
        return doc;
    }
}
