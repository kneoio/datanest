package com.semantyca.datanest.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.FileMetadata;
import com.semantyca.core.model.cnst.FileStorageType;
import com.semantyca.core.model.cnst.FileType;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.embedded.DocumentAccessInfo;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.exception.DocumentModificationAccessException;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.core.repository.rls.RlsActionUtil;
import com.semantyca.core.repository.table.EntityData;
import com.semantyca.datanest.dto.brand.mixdeck.BrandScriptEntryMixdeckDTO;
import com.semantyca.mixpla.model.brand.AiOverriding;
import com.semantyca.mixpla.model.brand.Brand;
import com.semantyca.mixpla.model.brand.BrandScriptEntry;
import com.semantyca.mixpla.model.brand.Owner;
import com.semantyca.mixpla.model.brand.ProfileOverriding;
import com.semantyca.mixpla.model.brand.StreamHistoryEntry;
import com.semantyca.mixpla.model.brand.StreamingOptions;
import com.semantyca.mixpla.model.cnst.ChatFeatureFlag;
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
import io.vertx.mutiny.sqlclient.SqlClient;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.semantyca.mixpla.repository.MixplaNameResolver.LISTENER;
import static com.semantyca.mixpla.repository.MixplaNameResolver.RADIO_STATION;
import static com.semantyca.mixpla.repository.MixplaNameResolver.SOUND_FRAGMENT;

@ApplicationScoped
public class BrandRepository extends AsyncRepository {
    private static final Logger LOGGER = Logger.getLogger(BrandRepository.class);
    private static final EntityData entityData = MixplaNameResolver.create().getEntityNames(RADIO_STATION);
    private static final EntityData soundFragmentEntityData = MixplaNameResolver.create().getEntityNames(SOUND_FRAGMENT);
    private static final EntityData listenerEntityData = MixplaNameResolver.create().getEntityNames(LISTENER);

    @Inject
    public BrandRepository(Pool client, ObjectMapper mapper, RLSRepository rlsRepository) {
        super(client, mapper, rlsRepository);
    }

    public Uni<List<Brand>> getAll(int limit, int offset, boolean includeArchived, final IUser user, BrandFilter filter) {
        StringBuilder sql = new StringBuilder();
        boolean hasSearchTerm = filter != null && filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty();

        sql.append("SELECT t.*, rls.*");
        if (hasSearchTerm) {
            sql.append(", similarity(t.search_name, $2) AS sim");
        }
        sql.append(" FROM ").append(entityData.getTableName()).append(" t, ")
                .append(entityData.getRlsName()).append(" rls ")
                .append("WHERE t.id = rls.entity_id AND rls.reader = $1");

        if (!includeArchived) {
            sql.append(" AND t.archived = 0");
        }

        if (filter != null && filter.isActivated()) {
            sql.append(buildFilterConditions(filter));
        }

        if (hasSearchTerm) {
            sql.append(" ORDER BY sim DESC");
        } else {
            sql.append(" ORDER BY t.last_mod_date DESC");
        }
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

    public Uni<Brand> insert(Brand station, List<RlsActionDTO> rlsActions, IUser user) {
        return Uni.createFrom().deferred(() -> {
            String sql = "INSERT INTO " + entityData.getTableName() +
                    " (author, reg_date, last_mod_user, last_mod_date, country, time_zone, managing_mode, color, loc_name, ai_overriding, profile_overriding, bit_rate, genres, slug_name, description, profile_id, ai_agent_id, one_time_stream_policy, submission_policy, messaging_policy, title_font, popularity_rate, public, owner, script_mode, streaming_options, custom_script_id, chat_feature_flags) " +
                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28) RETURNING id";

            OffsetDateTime now = OffsetDateTime.now();
            JsonObject localizedNameJson = JsonObject.mapFrom(station.getLocalizedName());
            JsonArray bitRateArray = JsonArray.of(station.getBitRate());
            JsonArray genresArray = station.getGenres() != null
                    ? new JsonArray(station.getGenres().stream().map(UUID::toString).toList())
                    : new JsonArray();

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
                    .addJsonArray(genresArray)
                    .addString(station.getSlugName())
                    .addString(station.getDescription())
                    .addUUID(station.getProfileId())
                    .addUUID(station.getAiAgentId())
                    .addString(station.getOneTimeStreamPolicy().name())
                    .addString(station.getSubmissionPolicy().name())
                    .addString(station.getMessagingPolicy().name())
                    .addString(station.getTitleFont())
                    .addDouble(station.getPopularityRate())
                    .addInteger(station.getPublicBrand())
                    .addJsonObject(station.getOwner() != null ? JsonObject.mapFrom(station.getOwner()) : new JsonObject())
                    .addString(station.getScriptMode() != null ? station.getScriptMode() : "PREDEFINED")
                    .addJsonObject(station.getStreamingOptions() != null ? JsonObject.mapFrom(station.getStreamingOptions()) : new JsonObject())
                    .addUUID(station.getCustomScriptId())
                    .addJsonObject(toChatFeatureFlagsJson(station.getChatFeatureFlags()));

            return client.withTransaction(tx ->
                            tx.preparedQuery(sql)
                                    .execute(params)
                                    .onItem().transform(result -> result.iterator().next().getUUID("id"))
                                    .onItem().transformToUni(id ->
                                            insertRLSPermissions(tx, id, entityData, user)
                                                    .onItem().transform(v -> id)
                                    )
                                    .onItem().transformToUni(id ->
                                            insertCoOwnerRLSPermissions(tx, id, entityData, extractCoOwnerIds(station))
                                                    .onItem().transform(v -> id)
                                    )
                                    .onItem().transformToUni(id ->
                                            updateBrandScripts(tx, id, station.getScriptIds())
                                                    .onItem().transform(v -> id)
                                    )
                                    .onItem().transformToUni(id ->
                                            upsertLabels(tx, id, station.getLabels())
                                                    .onItem().transform(v -> id)
                                    )
                                    .onItem().transformToUni(id ->
                                            applyRlsActions(tx, id, rlsActions)
                                                    .onItem().transform(v -> id)
                                    )
                    )
                    .onItem().transformToUni(id -> findById(id, user, true));
        });
    }

    public Uni<Brand> update(UUID id, Brand station, List<RlsActionDTO> rlsActions, IUser user) {
        return rlsRepository.findById(entityData.getRlsName(), user.getId(), id)
                .onItem().transformToUni(permissions -> {
                    if (!permissions[0]) {
                        return Uni.createFrom().failure(new DocumentModificationAccessException("User does not have edit permission", user.getUserName(), id));
                    }

                    String sql = "UPDATE " + entityData.getTableName() +
                            " SET country=$1, time_zone=$2, managing_mode=$3, color=$4, loc_name=$5, ai_overriding=$6, profile_overriding=$7, " +
                            "bit_rate=$8, genres=$9, slug_name=$10, description=$11, profile_id=$12, ai_agent_id=$13, one_time_stream_policy=$14::submission_policy, submission_policy=$15, messaging_policy=$16, title_font=$17, public=$18, last_mod_user=$19, last_mod_date=$20, owner=$21, script_mode=$22, streaming_options=$23, custom_script_id=$24, chat_feature_flags=$25 " +
                            "WHERE id=$26";

                    OffsetDateTime now = OffsetDateTime.now();
                    JsonObject localizedNameJson = JsonObject.mapFrom(station.getLocalizedName());
                    JsonArray bitRateArray = JsonArray.of(station.getBitRate());
                    JsonArray genresArray = station.getGenres() != null
                            ? new JsonArray(station.getGenres().stream().map(UUID::toString).toList())
                            : new JsonArray();

                    Tuple params = Tuple.tuple()
                            .addString(station.getCountry().name())
                            .addString(station.getTimeZone().getId())
                            .addString(station.getManagedBy().name())
                            .addString(station.getColor())
                            .addJsonObject(localizedNameJson)
                            .addJsonObject(station.getAiOverriding() != null ? JsonObject.mapFrom(station.getAiOverriding()) : new JsonObject())
                            .addJsonObject(station.getProfileOverriding() != null ? JsonObject.mapFrom(station.getProfileOverriding()) : new JsonObject())
                            .addJsonArray(bitRateArray)
                            .addJsonArray(genresArray)
                            .addString(station.getSlugName())
                            .addString(station.getDescription())
                            .addUUID(station.getProfileId())
                            .addUUID(station.getAiAgentId())
                            .addString(station.getOneTimeStreamPolicy().name())
                            .addString(station.getSubmissionPolicy().name())
                            .addString(station.getMessagingPolicy().name())
                            .addString(station.getTitleFont())
                            .addInteger(station.getPublicBrand())
                            .addLong(user.getId())
                            .addOffsetDateTime(now)
                            .addJsonObject(station.getOwner() != null ? JsonObject.mapFrom(station.getOwner()) : new JsonObject())
                            .addString(station.getScriptMode() != null ? station.getScriptMode() : "PREDEFINED")
                            .addJsonObject(station.getStreamingOptions() != null ? JsonObject.mapFrom(station.getStreamingOptions()) : new JsonObject())
                            .addUUID(station.getCustomScriptId())
                            .addJsonObject(toChatFeatureFlagsJson(station.getChatFeatureFlags()))
                            .addUUID(id);

                    return client.withTransaction(tx ->
                            tx.preparedQuery(sql)
                                    .execute(params)
                                    .onItem().transformToUni(rowSet -> {
                                        if (rowSet.rowCount() == 0) {
                                            return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                                        }
                                        return updateBrandScripts(tx, id, station.getScriptIds())
                                                .onItem().transformToUni(v -> upsertLabels(tx, id, station.getLabels()))
                                                .onItem().transformToUni(v -> applyRlsActions(tx, id, rlsActions))
                                                .onItem().transformToUni(v -> insertCoOwnerRLSPermissions(tx, id, entityData, extractCoOwnerIds(station)))
                                                .onItem().transformToUni(v -> backfillFragmentRlsForBrandMembers(tx, id))
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
            doc.setBitRate(64000);
        }

        JsonArray genresJson = row.getJsonArray("genres");
        if (genresJson != null && !genresJson.isEmpty()) {
            doc.setGenres(genresJson.stream()
                    .filter(String.class::isInstance)
                    .map(String.class::cast)
                    .map(UUID::fromString)
                    .toList());
        } else {
            doc.setGenres(List.of());
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
        String scriptMode = row.getString("script_mode");
        doc.setScriptMode(scriptMode != null ? scriptMode : "PREDEFINED");
        doc.setCustomScriptId(row.getUUID("custom_script_id"));

        JsonObject streamingOptionsJson = row.getJsonObject("streaming_options");
        if (streamingOptionsJson != null && !streamingOptionsJson.isEmpty()) {
            try {
                doc.setStreamingOptions(mapper.treeToValue(mapper.valueToTree(streamingOptionsJson.getMap()), StreamingOptions.class));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        JsonObject chatFeatureFlagsJson = row.getJsonObject("chat_feature_flags");
        if (chatFeatureFlagsJson != null && !chatFeatureFlagsJson.isEmpty()) {
            Map<ChatFeatureFlag, Boolean> chatFeatureFlags = new EnumMap<>(ChatFeatureFlag.class);
            chatFeatureFlagsJson.getMap().forEach((key, value) -> {
                try {
                    chatFeatureFlags.put(ChatFeatureFlag.valueOf(key), (Boolean) value);
                } catch (IllegalArgumentException ignored) {
                }
            });
            doc.setChatFeatureFlags(chatFeatureFlags);
        }

        JsonArray streamHistoryJson = row.getJsonArray("stream_history");
        if (streamHistoryJson != null && !streamHistoryJson.isEmpty()) {
            try {
                List<StreamHistoryEntry> streamHistory = mapper.readValue(streamHistoryJson.encode(), new TypeReference<>() {
                });
                doc.setStreamHistory(streamHistory);
            } catch (JsonProcessingException e) {
                doc.setStreamHistory(List.of());
            }
        }

        Uni<Brand> uni = Uni.createFrom().item(doc);

        if (includeLabels) {
            uni = uni.chain(d -> loadLabels(d.getId()).onItem().transform(labels -> {
                d.setLabels(labels);
                return d;
            }));
            uni = uni.chain(d -> loadLogoFiles(d.getId()).onItem().transform(files -> {
                d.setFileMetadataList(files);
                return d;
            }));
        } else {
            doc.setLabels(List.of());
            doc.setFileMetadataList(List.of());
        }

        return uni;
    }

    private Uni<List<UUID>> loadLabels(UUID brandId) {
        String sql = "SELECT label_id FROM mixpla__brand_labels WHERE brand_id = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(brandId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(row -> row.getUUID("label_id"))
                .collect().asList();
    }

    private Uni<List<FileMetadata>> loadLogoFiles(UUID brandId) {
        String sql = "SELECT id, reg_date, last_mod_date, parent_table, parent_id, archived, archived_date, " +
                "storage_type, mime_type, file_type, slug_name, file_original_name, file_key " +
                "FROM _files WHERE parent_table = '" + entityData.getTableName() + "' AND parent_id = $1 AND archived = 0";
        return client.preparedQuery(sql)
                .execute(Tuple.of(brandId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(row -> {
                    FileMetadata meta = new FileMetadata();
                    meta.setId(row.getLong("id"));
                    meta.setRegDate(row.getOffsetDateTime("reg_date").toZonedDateTime());
                    meta.setLastModifiedDate(row.getOffsetDateTime("last_mod_date").toZonedDateTime());
                    meta.setParentTable(row.getString("parent_table"));
                    meta.setParentId(row.getUUID("parent_id"));
                    meta.setArchived(row.getInteger("archived"));
                    if (row.getOffsetDateTime("archived_date") != null)
                        meta.setArchivedDate(row.getOffsetDateTime("archived_date"));
                    meta.setFileStorageType(FileStorageType.valueOf(row.getString("storage_type")));
                    meta.setMimeType(row.getString("mime_type"));
                    Integer fileTypeCode = row.getInteger("file_type");
                    if (fileTypeCode != null && fileTypeCode != 0) {
                        try { meta.setFileType(FileType.fromCode(fileTypeCode)); } catch (IllegalArgumentException ignored) {}
                    }
                    meta.setSlugName(row.getString("slug_name"));
                    meta.setFileOriginalName(row.getString("file_original_name"));
                    meta.setFileKey(row.getString("file_key"));
                    return meta;
                })
                .collect().asList();
    }

    public Uni<Void> upsertLogoFile(UUID brandId, FileMetadata meta) {
        String deleteSql = "DELETE FROM _files WHERE parent_id = $1 AND parent_table = '" + entityData.getTableName() + "'";
        OffsetDateTime now = ZonedDateTime.now(ZoneOffset.UTC).toOffsetDateTime();
        String insertSql = "INSERT INTO _files (parent_table, parent_id, storage_type, mime_type, file_type, " +
                "file_original_name, file_key, slug_name, reg_date, last_mod_date) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        Tuple params = Tuple.of(
                        entityData.getTableName(),
                        brandId,
                        FileStorageType.HETZNER.name(),
                        meta.getMimeType(),
                        FileType.BRAND_LOGO.getCode()
                )
                .addString(meta.getFileOriginalName())
                .addString(meta.getFileKey())
                .addString(meta.getSlugName())
                .addOffsetDateTime(now)
                .addOffsetDateTime(now);
        return client.withTransaction(tx ->
                tx.preparedQuery(deleteSql).execute(Tuple.of(brandId))
                        .onItem().transformToUni(ignored -> tx.preparedQuery(insertSql).execute(params))
                        .replaceWithVoid()
        );
    }

    public Uni<FileMetadata> getLogoFileBySlugName(UUID brandId, String slugName) {
        String sql = "SELECT id, mime_type, file_key, slug_name, file_original_name, storage_type, file_type " +
                "FROM _files WHERE parent_id = $1 AND slug_name = $2 AND parent_table = '" + entityData.getTableName() + "' AND archived = 0";
        return client.preparedQuery(sql)
                .execute(Tuple.of(brandId, slugName))
                .onItem().transformToUni(rows -> {
                    if (!rows.iterator().hasNext()) {
                        return Uni.createFrom().failure(new DocumentHasNotFoundException(brandId));
                    }
                    Row row = rows.iterator().next();
                    FileMetadata meta = new FileMetadata();
                    meta.setId(row.getLong("id"));
                    meta.setMimeType(row.getString("mime_type"));
                    meta.setFileKey(row.getString("file_key"));
                    meta.setSlugName(row.getString("slug_name"));
                    meta.setFileOriginalName(row.getString("file_original_name"));
                    meta.setFileStorageType(FileStorageType.valueOf(row.getString("storage_type")));
                    meta.setFileType(FileType.BRAND_LOGO);
                    return Uni.createFrom().item(meta);
                });
    }

    private Uni<Void> upsertLabels(io.vertx.mutiny.sqlclient.SqlClient tx, UUID brandId, List<UUID> labels) {
        if (labels == null || labels.isEmpty()) {
            return tx.preparedQuery("DELETE FROM mixpla__brand_labels WHERE brand_id = $1")
                    .execute(Tuple.of(brandId))
                    .replaceWithVoid();
        }

        String deleteSql = "DELETE FROM mixpla__brand_labels WHERE brand_id = $1";
        String insertSql = "INSERT INTO mixpla__brand_labels (brand_id, label_id) VALUES ($1, $2) ON CONFLICT DO NOTHING";

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

    // Re-check on brand save: grant every current owner + co-owner full fragment RLS on every
    // sound fragment already assigned to this brand, so a newly added co-owner retroactively sees
    // songs saved before they joined. Never revokes (deliberate — see the knowledge bundle
    // workflows/brand-team-visibility.md).
    private Uni<Void> backfillFragmentRlsForBrandMembers(SqlClient tx, UUID brandId) {
        String sfRls = soundFragmentEntityData.getRlsName();
        String ownerSql = "INSERT INTO " + sfRls + " (reader, entity_id, can_edit, can_delete) " +
                "SELECT (b.owner->>'userId')::bigint, bsf.sound_fragment_id, true, true " +
                "FROM mixpla__brands b JOIN mixpla__brand_sound_fragments bsf ON bsf.brand_id = b.id " +
                "WHERE b.id = $1 AND b.owner->>'userId' IS NOT NULL " +
                "ON CONFLICT DO NOTHING";
        String coOwnersSql = "INSERT INTO " + sfRls + " (reader, entity_id, can_edit, can_delete) " +
                "SELECT (co->>'userId')::bigint, bsf.sound_fragment_id, true, true " +
                "FROM mixpla__brands b JOIN mixpla__brand_sound_fragments bsf ON bsf.brand_id = b.id, " +
                "jsonb_array_elements(COALESCE(b.owner->'coOwners', '[]'::jsonb)) co " +
                "WHERE b.id = $1 AND co->>'userId' IS NOT NULL " +
                "ON CONFLICT DO NOTHING";
        return tx.preparedQuery(ownerSql).execute(Tuple.of(brandId))
                .chain(() -> tx.preparedQuery(coOwnersSql).execute(Tuple.of(brandId)))
                .replaceWithVoid();
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
                                "(SELECT sound_fragment_id FROM mixpla__brand_sound_fragments WHERE brand_id = $2)",
                                soundFragmentEntityData.getRlsName());

                        String removeListenerRlsSql = String.format(
                                "DELETE FROM %s WHERE reader = $1 AND entity_id IN " +
                                "(SELECT listener_id FROM mixpla__listener_brands WHERE brand_id = $2)",
                                listenerEntityData.getRlsName());

                        String removeSfAssocSql =
                                "DELETE FROM mixpla__brand_sound_fragments WHERE brand_id = $1";

                        String removeListenerAssocSql =
                                "DELETE FROM mixpla__listener_brands WHERE brand_id = $1";

                        String removeBrandRlsSql = String.format(
                                "DELETE FROM %s WHERE reader = $1 AND entity_id = $2",
                                entityData.getRlsName());

                        String setArchivedSql = String.format(
                                "UPDATE %s SET archived = 2, last_mod_user = $1, last_mod_date = $2 WHERE id = $3",
                                entityData.getTableName());

                        OffsetDateTime now = OffsetDateTime.now();

                        return tx.preparedQuery(removeSfRlsSql).execute(Tuple.of(user.getId(), id))
                                .onItem().transformToUni(ignored ->
                                        ensureSuperUserAccessForQuery(tx, soundFragmentEntityData.getRlsName(),
                                                "SELECT sound_fragment_id FROM mixpla__brand_sound_fragments WHERE brand_id = $1", id))
                                .onItem().transformToUni(ignored ->
                                        tx.preparedQuery(removeListenerRlsSql).execute(Tuple.of(user.getId(), id)))
                                .onItem().transformToUni(ignored ->
                                        ensureSuperUserAccessForQuery(tx, listenerEntityData.getRlsName(),
                                                "SELECT listener_id FROM mixpla__listener_brands WHERE brand_id = $1", id))
                                .onItem().transformToUni(ignored ->
                                        tx.preparedQuery(removeSfAssocSql).execute(Tuple.of(id)))
                                .onItem().transformToUni(ignored ->
                                        tx.preparedQuery(removeListenerAssocSql).execute(Tuple.of(id)))
                                .onItem().transformToUni(ignored ->
                                        tx.preparedQuery(removeBrandRlsSql).execute(Tuple.of(user.getId(), id)))
                                .onItem().transformToUni(ignored ->
                                        RlsActionUtil.ensureSuperUserAccess(tx, entityData.getRlsName(), id))
                                .onItem().transformToUni(ignored ->
                                        tx.preparedQuery(setArchivedSql).execute(Tuple.of(user.getId(), now, id)))
                                .onItem().transform(RowSet::rowCount);
                    });
                });
    }

    private Uni<Void> ensureSuperUserAccessForQuery(SqlClient tx, String rlsTable, String entityIdSql, UUID brandId) {
        return tx.preparedQuery(entityIdSql)
                .execute(Tuple.of(brandId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transformToUniAndConcatenate(row ->
                        RlsActionUtil.ensureSuperUserAccess(tx, rlsTable, row.getUUID(0)))
                .collect().asList()
                .replaceWithVoid();
    }

    public Uni<Integer> delete(UUID id, IUser user) {
        return rlsRepository.findById(entityData.getRlsName(), user.getId(), id)
                .onItem().transformToUni(permissions -> {
                    if (!permissions[1]) {
                        return Uni.createFrom().failure(new DocumentModificationAccessException("User does not have delete permission", user.getUserName(), id));
                    }

                    return client.withTransaction(tx -> {
                        String deleteRlsSql = String.format("DELETE FROM %s WHERE entity_id = $1", entityData.getRlsName());
                        String deleteFilesSql = "DELETE FROM _files WHERE parent_id = $1 AND parent_table = '" + entityData.getTableName() + "'";
                        String deleteDocSql = String.format("DELETE FROM %s WHERE id = $1", entityData.getTableName());

                        return tx.preparedQuery(deleteRlsSql)
                                .execute(Tuple.of(id))
                                .onItem().transformToUni(ignored ->
                                        tx.preparedQuery(deleteFilesSql).execute(Tuple.of(id))
                                )
                                .onItem().transformToUni(ignored ->
                                        tx.preparedQuery(deleteDocSql)
                                                .execute(Tuple.of(id))
                                )
                                .onItem().transform(RowSet::rowCount);
                    });
                });
    }

    public Uni<List<Brand>> getAllOpenForSubmission(int limit, int offset, long currentUserId) {
        String sql = "SELECT t.* FROM " + entityData.getTableName() + " t" +
                " WHERE t.archived = 0" +
                " AND t.submission_policy = 'NO_RESTRICTIONS'" +
                " AND ((t.owner->>'userId')::bigint IS NULL OR (t.owner->>'userId')::bigint != $1)" +
                " ORDER BY t.last_mod_date DESC" +
                (limit > 0 ? " LIMIT " + limit + " OFFSET " + offset : "");

        return client.preparedQuery(sql)
                .execute(Tuple.of(currentUserId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transformToUniAndConcatenate(row -> from(row, false))
                .collect().asList();
    }

    public Uni<Integer> getAllOpenForSubmissionCount(long currentUserId) {
        String sql = "SELECT COUNT(*) FROM " + entityData.getTableName() + " t" +
                " WHERE t.archived = 0" +
                " AND t.submission_policy = 'NO_RESTRICTIONS'" +
                " AND ((t.owner->>'userId')::bigint IS NULL OR (t.owner->>'userId')::bigint != $1)";

        return client.preparedQuery(sql)
                .execute(Tuple.of(currentUserId))
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<List<DocumentAccessInfo>> getDocumentAccessInfo(UUID documentId, IUser user) {
        return getDocumentAccessInfo(documentId, entityData, user);
    }

    public Uni<List<BrandScriptEntry>> getScriptEntriesForBrand(UUID brandId) {
        String sql = "SELECT script_id, user_variables FROM mixpla__brand_scripts WHERE brand_id = $1 ORDER BY rank";
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

    public Uni<List<BrandScriptEntryMixdeckDTO>> getScriptEntryDTOsForBrand(UUID brandId) {
        String sql = "SELECT s.slug_name, bs.user_variables " +
                "FROM mixpla__brand_scripts bs " +
                "JOIN mixpla__scripts s ON s.id = bs.script_id " +
                "WHERE bs.brand_id = $1 ORDER BY bs.rank";
        return client.preparedQuery(sql)
                .execute(Tuple.of(brandId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(row -> {
                    BrandScriptEntryMixdeckDTO entry = new BrandScriptEntryMixdeckDTO();
                    entry.setSlugName(row.getString("slug_name"));
                    JsonObject userVarsJson = row.getJsonObject("user_variables");
                    if (userVarsJson != null) {
                        entry.setUserVariables(userVarsJson.getMap());
                    }
                    return entry;
                })
                .collect().asList();
    }

    private Uni<UUID> updateBrandScripts(io.vertx.mutiny.sqlclient.SqlClient tx, UUID brandId, List<BrandScriptEntry> scripts) {
        if (scripts == null) {
            return Uni.createFrom().item(brandId);
        }
        String deleteSql = "DELETE FROM mixpla__brand_scripts WHERE brand_id = $1";
        String insertSql = "INSERT INTO mixpla__brand_scripts (brand_id, script_id, user_variables, rank) VALUES ($1, $2, $3, $4)";

        return tx.preparedQuery(deleteSql)
                .execute(Tuple.of(brandId))
                .onItem().transformToUni(deleteResult -> {
                    if (scripts.isEmpty()) {
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
            conditions.append(" AND EXISTS (SELECT 1 FROM mixpla__brand_labels bl WHERE bl.brand_id = t.id AND bl.label_id IN (");
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

        if (filter.getOneTimeStreamPolicy() != null) {
            conditions.append(" AND t.one_time_stream_policy = '")
                    .append(filter.getOneTimeStreamPolicy().name())
                    .append("'");
        }
        if (filter.getSubmissionPolicy() != null) {
            conditions.append(" AND t.submission_policy = '")
                    .append(filter.getSubmissionPolicy().name())
                    .append("'");
        }
        if (filter.getMessagingPolicy() != null) {
            conditions.append(" AND t.messaging_policy = '")
                    .append(filter.getMessagingPolicy().name())
                    .append("'");
        }

        if (filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty()) {
            conditions.append(" AND (t.search_name ILIKE '%' || $2 || '%' OR similarity(t.search_name, $2) > 0.05)");
        }

        return conditions.toString();
    }

    private void addFilterParameters(Tuple params, BrandFilter filter) {
        if (filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty()) {
            params.addString(filter.getSearchTerm());
        }
    }

    private JsonObject toChatFeatureFlagsJson(Map<ChatFeatureFlag, Boolean> chatFeatureFlags) {
        JsonObject json = new JsonObject();
        if (chatFeatureFlags != null) {
            chatFeatureFlags.forEach((flag, value) -> json.put(flag.name(), value));
        }
        return json;
    }

    private List<Long> extractCoOwnerIds(Brand brand) {
        if (brand.getOwner() == null) {
            return List.of();
        }
        List<Long> ids = new ArrayList<>();
        if (brand.getOwner().getUserId() != null) {
            ids.add(brand.getOwner().getUserId());
        }
        if (brand.getOwner().getCoOwners() != null) {
            for (Owner co : brand.getOwner().getCoOwners()) {
                if (co.getUserId() != null) {
                    ids.add(co.getUserId());
                }
            }
        }
        return ids;
    }
}
