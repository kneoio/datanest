package com.semantyca.datanest.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.embedded.DocumentAccessInfo;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.exception.DocumentModificationAccessException;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.core.repository.table.EntityData;
import com.semantyca.mixpla.model.brand.*;
import com.semantyca.mixpla.model.cnst.ManagedBy;
import com.semantyca.mixpla.model.cnst.SubmissionPolicy;
import com.semantyca.mixpla.model.filter.BrandFilter;
import com.semantyca.mixpla.repository.MixplaNameResolver;
import com.semantyca.officeframe.model.cnst.CountryCode;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.OffsetDateTime;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;

import static com.semantyca.mixpla.repository.MixplaNameResolver.*;
import static com.semantyca.mixpla.repository.MixplaNameResolver.RADIO_STATION;

@ApplicationScoped
public class BrandRepository extends AsyncRepository {
    private static final Logger LOGGER = Logger.getLogger(BrandRepository.class);
    private static final EntityData entityData = MixplaNameResolver.create().getEntityNames(RADIO_STATION);
    private static final EntityData brandStats = MixplaNameResolver.create().getEntityNames(BRAND_STATS);
    private static final EntityData soundFragmentEntityData = MixplaNameResolver.create().getEntityNames(SOUND_FRAGMENT);
    private static final EntityData listenerEntityData = MixplaNameResolver.create().getEntityNames(LISTENER);

    @Inject
    public BrandRepository(Pool client, ObjectMapper mapper, RLSRepository rlsRepository) {
        super(client, mapper, rlsRepository);
    }

    public Uni<List<Brand>> getAll(int limit, int offset, boolean includeArchived, final IUser user, BrandFilter filter) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ").append(entityData.getTableName()).append(" t, ")
                .append(entityData.getRlsName()).append(" rls ")
                .append("WHERE t.id = rls.entity_id AND rls.reader = $1");

        if (!includeArchived) {
            sql.append(" AND t.archived = 0");
        }

        if (filter != null && filter.isActivated()) {
            sql.append(buildFilterConditions(filter));
        }

        sql.append(" ORDER BY t.last_mod_date DESC");
        if (limit > 0) {
            sql.append(" LIMIT ").append(limit).append(" OFFSET ").append(offset);
        }

        Tuple params = Tuple.tuple().addLong(user.getId());

        if (filter != null && filter.isActivated()) {
            addFilterParameters(params, filter);
        }

        return client.preparedQuery(sql.toString())
                .execute(params)
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transformToUniAndConcatenate(row -> from(row, false))
                .collect().asList();
    }

    public Uni<Integer> getAllCount(IUser user, boolean includeArchived, BrandFilter filter) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT COUNT(*) FROM ").append(entityData.getTableName()).append(" t, ")
                .append(entityData.getRlsName()).append(" rls ")
                .append("WHERE t.id = rls.entity_id AND rls.reader = $1");

        if (!includeArchived) {
            sql.append(" AND t.archived = 0");
        }

        if (filter != null && filter.isActivated()) {
            sql.append(buildFilterConditions(filter));
        }

        Tuple params = Tuple.tuple().addLong(user.getId());

        if (filter != null && filter.isActivated()) {
            addFilterParameters(params, filter);
        }

        return client.preparedQuery(sql.toString())
                .execute(params)
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<Brand> findById(UUID id, IUser user, boolean includeArchived) {
        String sql = "SELECT theTable.*, rls.* " +
                "FROM %s theTable " +
                "JOIN %s rls ON theTable.id = rls.entity_id " +
                "WHERE rls.reader = $1 AND theTable.id = $2";

        if (!includeArchived) {
            sql += " AND theTable.archived = 0";
        }

        return client.preparedQuery(String.format(sql, entityData.getTableName(), entityData.getRlsName()))
                .execute(Tuple.of(user.getId(), id))
                .onItem().transform(RowSet::iterator)
                .onItem().transformToUni(iterator -> {
                    if (iterator.hasNext()) {
                        return from(iterator.next(), true);
                    } else {
                        return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                    }
                });
    }

    public Uni<Brand> getBySlugName(String name) {
        String sql = "SELECT * FROM " + entityData.getTableName() + " WHERE slug_name = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(name))
                .onItem().transform(RowSet::iterator)
                .onItem().transformToUni(iterator -> {
                    if (iterator.hasNext()) {
                        return from(iterator.next(), false);
                    } else {
                        return Uni.createFrom().failure(new DocumentHasNotFoundException(name));
                    }
                });
    }

    public Uni<Brand> getBySlugName(String name, IUser user, boolean includeArchived) {
        String sql = "SELECT theTable.*, rls.* " +
                "FROM %s theTable " +
                "JOIN %s rls ON theTable.id = rls.entity_id " +
                "WHERE rls.reader = $1 AND theTable.slug_name = $2";

        if (!includeArchived) {
            sql += " AND theTable.archived = 0";
        }

        return client.preparedQuery(String.format(sql, entityData.getTableName(), entityData.getRlsName()))
                .execute(Tuple.of(user.getId(), name))
                .onItem().transform(RowSet::iterator)
                .onItem().transformToUni(iterator -> {
                    if (iterator.hasNext()) {
                        return from(iterator.next(), true);
                    } else {
                        return Uni.createFrom().failure(new DocumentHasNotFoundException(name));
                    }
                });
    }

    public Uni<Brand> insert(Brand station, IUser user) {
        return Uni.createFrom().deferred(() -> {
            String sql = "INSERT INTO " + entityData.getTableName() +
                    " (author, reg_date, last_mod_user, last_mod_date, country, time_zone, managing_mode, color, loc_name, ai_overriding, profile_overriding, bit_rate, slug_name, description, profile_id, ai_agent_id, one_time_stream_policy, submission_policy, messaging_policy, title_font, popularity_rate, is_temporary, public, owner) " +
                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24) RETURNING id";

            OffsetDateTime now = OffsetDateTime.now();
            JsonObject localizedNameJson = JsonObject.mapFrom(station.getLocalizedName());
            JsonArray bitRateArray = JsonArray.of(station.getBitRate());

            Tuple params = Tuple.tuple()
                    .addLong(user.getId())
                    .addOffsetDateTime(now)
                    .addLong(user.getId())
                    .addOffsetDateTime(now)
                    .addString(station.getCountry() != null ? station.getCountry().name() : null)
                    .addString(station.getTimeZone().getId())
                    .addString(station.getManagedBy().name())
                    .addString(station.getColor())
                    .addJsonObject(localizedNameJson)
                    .addJsonObject(station.getAiOverriding() != null ? JsonObject.mapFrom(station.getAiOverriding()) : new JsonObject())
                    .addJsonObject(station.getProfileOverriding() != null ? JsonObject.mapFrom(station.getProfileOverriding()) : new JsonObject())
                    .addJsonArray(bitRateArray)
                    .addString(station.getSlugName())
                    .addString(station.getDescription())
                    .addUUID(station.getProfileId())
                    .addUUID(station.getAiAgentId())
                    .addString(station.getOneTimeStreamPolicy().name())
                    .addString(station.getSubmissionPolicy().name())
                    .addString(station.getMessagingPolicy().name())
                    .addString(station.getTitleFont())
                    .addDouble(station.getPopularityRate())
                    .addInteger(station.getIsTemporary())
                    .addInteger(station.getPublicBrand())
                    .addJsonObject(station.getOwner() != null ? JsonObject.mapFrom(station.getOwner()) : new JsonObject());

            return client.withTransaction(tx ->
                            tx.preparedQuery(sql)
                                    .execute(params)
                                    .onItem().transform(result -> result.iterator().next().getUUID("id"))
                                    .onItem().transformToUni(id ->
                                            insertRLSPermissions(tx, id, entityData, user)
                                                    .onItem().transform(v -> id)
                                    )
                                    .onItem().transformToUni(id ->
                                            updateBrandScripts(tx, id, station.getScripts())
                                                    .onItem().transform(v -> id)
                                    )
                                    .onItem().transformToUni(id ->
                                            upsertLabels(tx, id, station.getLabels())
                                                    .onItem().transform(v -> id)
                                    )
                    )
                    .onItem().transformToUni(id -> findById(id, user, true));
        });
    }

    public Uni<Brand> update(UUID id, Brand station, IUser user) {
        return rlsRepository.findById(entityData.getRlsName(), user.getId(), id)
                .onItem().transformToUni(permissions -> {
                    if (!permissions[0]) {
                        return Uni.createFrom().failure(new DocumentModificationAccessException("User does not have edit permission", user.getUserName(), id));
                    }

                    String sql = "UPDATE " + entityData.getTableName() +
                            " SET country=$1, time_zone=$2, managing_mode=$3, color=$4, loc_name=$5, ai_overriding=$6, profile_overriding=$7, " +
                            "bit_rate=$8, slug_name=$9, description=$10, profile_id=$11, ai_agent_id=$12, one_time_stream_policy=$13::submission_policy, submission_policy=$14, messaging_policy=$15, title_font=$16, is_temporary=$17, public=$18, last_mod_user=$19, last_mod_date=$20, owner=$21 " +
                            "WHERE id=$22";

                    OffsetDateTime now = OffsetDateTime.now();
                    JsonObject localizedNameJson = JsonObject.mapFrom(station.getLocalizedName());
                    JsonArray bitRateArray = JsonArray.of(station.getBitRate());

                    Tuple params = Tuple.tuple()
                            .addString(station.getCountry().name())
                            .addString(station.getTimeZone().getId())
                            .addString(station.getManagedBy().name())
                            .addString(station.getColor())
                            .addJsonObject(localizedNameJson)
                            .addJsonObject(station.getAiOverriding() != null ? JsonObject.mapFrom(station.getAiOverriding()) : new JsonObject())
                            .addJsonObject(station.getProfileOverriding() != null ? JsonObject.mapFrom(station.getProfileOverriding()) : new JsonObject())
                            .addJsonArray(bitRateArray)
                            .addString(station.getSlugName())
                            .addString(station.getDescription())
                            .addUUID(station.getProfileId())
                            .addUUID(station.getAiAgentId())
                            .addString(station.getOneTimeStreamPolicy().name())
                            .addString(station.getSubmissionPolicy().name())
                            .addString(station.getMessagingPolicy().name())
                            .addString(station.getTitleFont())
                            .addInteger(station.getIsTemporary() != null ? station.getIsTemporary() : 0)
                            .addInteger(station.getPublicBrand())
                            .addLong(user.getId())
                            .addOffsetDateTime(now)
                            .addJsonObject(station.getOwner() != null ? JsonObject.mapFrom(station.getOwner()) : new JsonObject())
                            .addUUID(id);

                    return client.withTransaction(tx ->
                            tx.preparedQuery(sql)
                                    .execute(params)
                                    .onItem().transformToUni(rowSet -> {
                                        if (rowSet.rowCount() == 0) {
                                            return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                                        }
                                        return updateBrandScripts(tx, id, station.getScripts())
                                                .onItem().transformToUni(v -> upsertLabels(tx, id, station.getLabels()))
                                                .onItem().transform(v -> id);
                                    })
                    ).onItem().transformToUni(stationId -> findById(stationId, user, true));
                });
    }

    private Uni<Brand> from(Row row, boolean includeLabels) {
        Brand doc = new Brand();
        setDefaultFields(doc, row);

        JsonObject localizedNameJson = row.getJsonObject(COLUMN_LOCALIZED_NAME);
        if (localizedNameJson != null) {
            EnumMap<LanguageCode, String> localizedName = new EnumMap<>(LanguageCode.class);
            localizedNameJson.getMap().forEach((key, value) ->
                    localizedName.put(LanguageCode.valueOf(key), (String) value));
            doc.setLocalizedName(localizedName);
        }

        doc.setSlugName(row.getString("slug_name"));
        doc.setArchived(row.getInteger("archived"));
        doc.setIsTemporary(row.getInteger("is_temporary"));
        doc.setPublicBrand(row.getInteger("public"));
        String country = row.getString("country");
        doc.setCountry(country != null ? CountryCode.valueOf(country) : null);
        doc.setManagedBy(ManagedBy.valueOf(row.getString("managing_mode")));
        doc.setTimeZone(java.time.ZoneId.of(row.getString("time_zone")));
        doc.setColor(row.getString("color"));
        doc.setDescription(row.getString("description"));
        doc.setOneTimeStreamPolicy(SubmissionPolicy.valueOf(row.getString("one_time_stream_policy")));
        doc.setSubmissionPolicy(SubmissionPolicy.valueOf(row.getString("submission_policy")));
        doc.setMessagingPolicy(SubmissionPolicy.valueOf(row.getString("messaging_policy")));
        doc.setTitleFont(row.getString("title_font"));

        JsonArray bitRateJson = row.getJsonArray("bit_rate");
        if (bitRateJson != null && !bitRateJson.isEmpty()) {
            doc.setBitRate(Long.parseLong(bitRateJson.getString(0)));
        } else {
            doc.setBitRate(128000);
        }

        JsonObject aiOverridingJson = row.getJsonObject("ai_overriding");
        if (!aiOverridingJson.isEmpty()) {
            try {
                AiOverriding ai = mapper.treeToValue(
                        mapper.valueToTree(aiOverridingJson.getMap()), AiOverriding.class);
                doc.setAiOverriding(ai);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        JsonObject profileOverridingJson = row.getJsonObject("profile_overriding");
        if (!profileOverridingJson.isEmpty()) {
            try {
                ProfileOverriding profile = mapper.treeToValue(
                        mapper.valueToTree(profileOverridingJson.getMap()), ProfileOverriding.class);
                doc.setProfileOverriding(profile);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        UUID aiAgentId = row.getUUID("ai_agent_id");
        if (aiAgentId != null) {
            doc.setAiAgentId(aiAgentId);
        }
        doc.setPopularityRate(row.getDouble("popularity_rate"));
        UUID profileId = row.getUUID("profile_id");
        if (profileId != null) {
            doc.setProfileId(profileId);
        }

        JsonObject ownerJson = row.getJsonObject("owner");
        if (ownerJson != null && !ownerJson.isEmpty()) {
            doc.setOwner(mapper.convertValue(ownerJson.getMap(), Owner.class));
        }

        Uni<Brand> uni = Uni.createFrom().item(doc);

        if (includeLabels) {
            uni = uni.chain(d -> loadLabels(d.getId()).onItem().transform(labels -> {
                d.setLabels(labels);
                return d;
            }));
        } else {
            doc.setLabels(List.of());
        }

        return uni;
    }

    private Uni<List<UUID>> loadLabels(UUID brandId) {
        String sql = "SELECT label_id FROM kneobroadcaster__brand_labels WHERE brand_id = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(brandId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(row -> row.getUUID("label_id"))
                .collect().asList();
    }

    private Uni<Void> upsertLabels(io.vertx.mutiny.sqlclient.SqlClient tx, UUID brandId, List<UUID> labels) {
        if (labels == null || labels.isEmpty()) {
            return tx.preparedQuery("DELETE FROM kneobroadcaster__brand_labels WHERE brand_id = $1")
                    .execute(Tuple.of(brandId))
                    .replaceWithVoid();
        }

        String deleteSql = "DELETE FROM kneobroadcaster__brand_labels WHERE brand_id = $1";
        String insertSql = "INSERT INTO kneobroadcaster__brand_labels (brand_id, label_id) VALUES ($1, $2) ON CONFLICT DO NOTHING";

        return tx.preparedQuery(deleteSql)
                .execute(Tuple.of(brandId))
                .chain(() -> Multi.createFrom().iterable(labels)
                        .onItem().transformToUni(labelId ->
                                tx.preparedQuery(insertSql).execute(Tuple.of(brandId, labelId))
                        )
                        .merge()
                        .collect().asList()
                        .replaceWithVoid());
    }

    public Uni<Integer> archive(UUID id, IUser user) {
        return archive(id, entityData, user);
    }

    public Uni<Integer> closeBrand(UUID id, IUser user) {
        return rlsRepository.findById(entityData.getRlsName(), user.getId(), id)
                .onItem().transformToUni(permissions -> {
                    if (!permissions[0]) {
                        return Uni.createFrom().failure(new DocumentModificationAccessException(
                                "User does not have edit permission", user.getUserName(), id));
                    }

                    return client.withTransaction(tx -> {
                        String removeSfRlsSql = String.format(
                                "DELETE FROM %s WHERE reader = $1 AND entity_id IN " +
                                "(SELECT sound_fragment_id FROM kneobroadcaster__brand_sound_fragments WHERE brand_id = $2)",
                                soundFragmentEntityData.getRlsName());

                        String removeListenerRlsSql = String.format(
                                "DELETE FROM %s WHERE reader = $1 AND entity_id IN " +
                                "(SELECT listener_id FROM kneobroadcaster__listener_brands WHERE brand_id = $2)",
                                listenerEntityData.getRlsName());

                        String removeSfAssocSql =
                                "DELETE FROM kneobroadcaster__brand_sound_fragments WHERE brand_id = $1";

                        String removeListenerAssocSql =
                                "DELETE FROM kneobroadcaster__listener_brands WHERE brand_id = $1";

                        String removeBrandRlsSql = String.format(
                                "DELETE FROM %s WHERE reader = $1 AND entity_id = $2",
                                entityData.getRlsName());

                        String setArchivedSql = String.format(
                                "UPDATE %s SET archived = 2, last_mod_user = $1, last_mod_date = $2 WHERE id = $3",
                                entityData.getTableName());

                        OffsetDateTime now = OffsetDateTime.now();

                        return tx.preparedQuery(removeSfRlsSql).execute(Tuple.of(user.getId(), id))
                                .onItem().transformToUni(ignored ->
                                        tx.preparedQuery(removeListenerRlsSql).execute(Tuple.of(user.getId(), id)))
                                .onItem().transformToUni(ignored ->
                                        tx.preparedQuery(removeSfAssocSql).execute(Tuple.of(id)))
                                .onItem().transformToUni(ignored ->
                                        tx.preparedQuery(removeListenerAssocSql).execute(Tuple.of(id)))
                                .onItem().transformToUni(ignored ->
                                        tx.preparedQuery(removeBrandRlsSql).execute(Tuple.of(user.getId(), id)))
                                .onItem().transformToUni(ignored ->
                                        tx.preparedQuery(setArchivedSql).execute(Tuple.of(user.getId(), now, id)))
                                .onItem().transform(RowSet::rowCount);
                    });
                });
    }

    public Uni<Integer> delete(UUID id, IUser user) {
        return rlsRepository.findById(entityData.getRlsName(), user.getId(), id)
                .onItem().transformToUni(permissions -> {
                    if (!permissions[1]) {
                        return Uni.createFrom().failure(new DocumentModificationAccessException("User does not have delete permission", user.getUserName(), id));
                    }

                    return client.withTransaction(tx -> {
                        String deleteRlsSql = String.format("DELETE FROM %s WHERE entity_id = $1", entityData.getRlsName());
                        String deleteDocSql = String.format("DELETE FROM %s WHERE id = $1", entityData.getTableName());

                        return tx.preparedQuery(deleteRlsSql)
                                .execute(Tuple.of(id))
                                .onItem().transformToUni(ignored ->
                                        tx.preparedQuery(deleteDocSql)
                                                .execute(Tuple.of(id))
                                )
                                .onItem().transform(RowSet::rowCount);
                    });
                });
    }

    public Uni<Void> upsertStationAccessWithCountAndGeo(String stationName, Long accessCount, OffsetDateTime lastAccessTime, String userAgent, String ipAddress, String countryCode) {
        String sql = "INSERT INTO " + brandStats.getTableName() +
                " (station_name, access_count, last_access_time, user_agent, ip_address, country_code) " +
                "VALUES ($1, $2, $3, $4, $5, $6) " +
                "ON CONFLICT (station_name, ip_address, country_code) " +
                "DO UPDATE SET access_count = EXCLUDED.access_count + " + brandStats.getTableName() + ".access_count, last_access_time = $3, user_agent = $4;";

        return client.preparedQuery(sql)
                .execute(Tuple.of(stationName, accessCount, lastAccessTime, userAgent, ipAddress, countryCode))
                .replaceWithVoid();
    }

    public Uni<OffsetDateTime> findLastAccessTimeByStationName(String stationName) {
        String sql = "SELECT last_access_time FROM " +
                brandStats.getTableName() + " WHERE station_name = $1 ORDER BY last_access_time DESC LIMIT 1";

        return client.preparedQuery(sql)
                .execute(Tuple.of(stationName))
                .onItem().transform(RowSet::iterator)
                .onItem().transform(iterator -> {
                    if (iterator.hasNext()) {
                        return iterator.next().getOffsetDateTime("last_access_time");
                    } else {
                        return null;
                    }
                });
    }

    public Uni<List<DocumentAccessInfo>> getDocumentAccessInfo(UUID documentId, IUser user) {
        return getDocumentAccessInfo(documentId, entityData, user);
    }

    public Uni<List<BrandScriptEntry>> getScriptEntriesForBrand(UUID brandId) {
        String sql = "SELECT script_id, user_variables FROM kneobroadcaster__brand_scripts WHERE brand_id = $1 ORDER BY rank";
        return client.preparedQuery(sql)
                .execute(Tuple.of(brandId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(row -> {
                    BrandScriptEntry entry = new BrandScriptEntry();
                    entry.setScriptId(row.getUUID("script_id"));
                    JsonObject userVarsJson = row.getJsonObject("user_variables");
                    if (userVarsJson != null) {
                        entry.setUserVariables(userVarsJson.getMap());
                    }
                    return entry;
                })
                .collect().asList();
    }

    private Uni<UUID> updateBrandScripts(io.vertx.mutiny.sqlclient.SqlClient tx, UUID brandId, List<BrandScriptEntry> scripts) {
        String deleteSql = "DELETE FROM kneobroadcaster__brand_scripts WHERE brand_id = $1";
        String insertSql = "INSERT INTO kneobroadcaster__brand_scripts (brand_id, script_id, user_variables, rank) VALUES ($1, $2, $3, $4)";

        return tx.preparedQuery(deleteSql)
                .execute(Tuple.of(brandId))
                .onItem().transformToUni(deleteResult -> {
                    if (scripts == null || scripts.isEmpty()) {
                        return Uni.createFrom().item(brandId);
                    }

                    List<Uni<Void>> insertUnis = new java.util.ArrayList<>();
                    for (int i = 0; i < scripts.size(); i++) {
                        BrandScriptEntry entry = scripts.get(i);
                        Tuple params = Tuple.tuple()
                                .addUUID(brandId)
                                .addUUID(entry.getScriptId())
                                .addJsonObject(entry.getUserVariables() != null ? JsonObject.mapFrom(entry.getUserVariables()) : new JsonObject())
                                .addInteger(i);
                        insertUnis.add(tx.preparedQuery(insertSql).execute(params).replaceWithVoid());
                    }

                    return Uni.join().all(insertUnis).andFailFast()
                            .onItem().transform(v -> brandId);
                });
    }

    private String buildFilterConditions(BrandFilter filter) {
        StringBuilder conditions = new StringBuilder();

        if (filter.getCountries() != null && !filter.getCountries().isEmpty()) {
            conditions.append(" AND t.country IN (");
            for (int i = 0; i < filter.getCountries().size(); i++) {
                if (i > 0) {
                    conditions.append(", ");
                }
                conditions.append("'").append(filter.getCountries().get(i).name()).append("'");
            }
            conditions.append(")");
        }

        if (filter.getLabels() != null && !filter.getLabels().isEmpty()) {
            conditions.append(" AND EXISTS (SELECT 1 FROM kneobroadcaster__brand_labels bl WHERE bl.brand_id = t.id AND bl.label_id IN (");
            for (int i = 0; i < filter.getLabels().size(); i++) {
                if (i > 0) {
                    conditions.append(", ");
                }
                conditions.append("'").append(filter.getLabels().get(i).toString()).append("'");
            }
            conditions.append("))");
        }

        if (filter.isPublicBrand()) {
            conditions.append(" AND t.public = 1");
        }

        return conditions.toString();
    }

    private void addFilterParameters(Tuple params, BrandFilter filter) {
        // No parameters to add for now - countries, labels, and publicBrand are inline in SQL
    }
}
