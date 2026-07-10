package com.semantyca.datanest.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.exception.DocumentModificationAccessException;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.core.repository.table.EntityData;
import com.semantyca.mixpla.model.filter.OtsDefinitionFilter;
import com.semantyca.mixpla.model.stream.OtsDefinition;
import com.semantyca.mixpla.repository.MixplaNameResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.semantyca.mixpla.repository.MixplaNameResolver.OTS_DEFINITION;

@ApplicationScoped
public class OtsDefinitionRepository extends AsyncRepository {
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

    public Uni<OtsDefinition> insert(OtsDefinition ots, IUser user) {
        return Uni.createFrom().deferred(() -> {
            try {
                String sql = "INSERT INTO " + entityData.getTableName() +
                        " (author, reg_date, last_mod_user, last_mod_date, name, slug_name, script_id, user_variables, brand_id, agent_id) " +
                        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id";

                OffsetDateTime now = OffsetDateTime.now();

                JsonObject userVarsJson = null;
                if (ots.getUserVariables() != null && !ots.getUserVariables().isEmpty()) {
                    userVarsJson = new JsonObject(mapper.writeValueAsString(ots.getUserVariables()));
                }

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
                        .addUUID(ots.getAgentId());

                return client.withTransaction(tx ->
                        tx.preparedQuery(sql)
                                .execute(params)
                                .onItem().transform(result -> result.iterator().next().getUUID("id"))
                                .onItem().transformToUni(id ->
                                        insertRLSPermissions(tx, id, entityData, user)
                                                .onItem().transform(ignored -> id)
                                )
                ).onItem().transformToUni(id -> findById(id, user));
            } catch (Exception e) {
                return Uni.createFrom().failure(e);
            }
        });
    }

    public Uni<OtsDefinition> findById(UUID id, IUser user) {
        String sql = """
                    SELECT t.*
                    FROM %s t
                    JOIN %s rls ON t.id = rls.entity_id
                    WHERE rls.reader = $1 AND t.id = $2
                """.formatted(entityData.getTableName(), entityData.getRlsName());

        return client.preparedQuery(sql)
                .execute(Tuple.of(user.getId(), id))
                .onItem().transform(RowSet::iterator)
                .onItem().transformToUni(iterator -> {
                    if (iterator.hasNext()) {
                        return Uni.createFrom().item(from(iterator.next()));
                    } else {
                        return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                    }
                });
    }

    public Uni<OtsDefinition> update(UUID id, OtsDefinition ots, IUser user) {
        return Uni.createFrom().deferred(() -> {
            try {
                JsonObject userVarsJson = null;
                if (ots.getUserVariables() != null && !ots.getUserVariables().isEmpty()) {
                    userVarsJson = new JsonObject(mapper.writeValueAsString(ots.getUserVariables()));
                }
                JsonObject finalUserVarsJson = userVarsJson;

                return rlsRepository.findById(entityData.getRlsName(), user.getId(), id)
                        .onItem().transformToUni(permissions -> {
                            if (!permissions[0]) {
                                return Uni.createFrom().failure(
                                        new DocumentModificationAccessException("User does not have edit permission", user.getUserName(), id)
                                );
                            }

                            String sql = "UPDATE " + entityData.getTableName() +
                                    " SET name=$1, script_id=$2, user_variables=$3, brand_id=$4, agent_id=$5, last_mod_user=$6, last_mod_date=$7 " +
                                    "WHERE id=$8";

                            OffsetDateTime now = OffsetDateTime.now();

                            Tuple params = Tuple.tuple()
                                    .addString(ots.getName())
                                    .addUUID(ots.getScriptId())
                                    .addJsonObject(finalUserVarsJson)
                                    .addUUID(ots.getBrandId())
                                    .addUUID(ots.getAgentId())
                                    .addLong(user.getId())
                                    .addOffsetDateTime(now)
                                    .addUUID(id);

                            return client.preparedQuery(sql)
                                    .execute(params)
                                    .onItem().transformToUni(rowSet -> {
                                        if (rowSet.rowCount() == 0) {
                                            return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                                        }
                                        return findById(id, user);
                                    });
                        });
            } catch (Exception e) {
                return Uni.createFrom().failure(e);
            }
        });
    }

    public Uni<Integer> archive(UUID id, IUser user) {
        return archive(id, entityData, user);
    }

    public Uni<Integer> delete(UUID id, IUser user) {
        return delete(id, entityData, user);
    }

    private OtsDefinition from(Row row) {
        OtsDefinition doc = new OtsDefinition();
        setDefaultFields(doc, row);
        doc.setName(row.getString("name"));
        doc.setSlugName(row.getString("slug_name"));
        doc.setScriptId(row.getUUID("script_id"));
        doc.setBrandId(row.getUUID("brand_id"));
        doc.setAgentId(row.getUUID("agent_id"));

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
