package com.semantyca.datanest.service;

import com.semantyca.core.model.user.IUser;
import com.semantyca.core.service.external.hetzner.HetznerStorageService;
import com.semantyca.datanest.messaging.MetricPublisher;
import com.semantyca.mixpla.dto.queue.metric.MetricEventType;
import com.semantyca.mixpla.dto.queue.metric.ProcessType;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.SqlClient;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@ApplicationScoped
public class DefaultSoundFragmentProvisionService {

    private static final Logger LOGGER = Logger.getLogger(DefaultSoundFragmentProvisionService.class);
    private static final String DEFAULT_ARTIST = "default_mixpla";
    private static final long SYSTEM_USER_ID = 1L;
    private static final String SF_TABLE = "mixpla__sound_fragments";
    private static final String RLS_TABLE = "mixpla__sound_fragment_readers";

    private final Pool client;
    private final HetznerStorageService fileStorage;
    private final MetricPublisher metricPublisher;

    DefaultSoundFragmentProvisionService() {
        this.client = null;
        this.fileStorage = null;
        this.metricPublisher = null;
    }

    @Inject
    public DefaultSoundFragmentProvisionService(Pool client, HetznerStorageService fileStorage, MetricPublisher metricPublisher) {
        this.client = client;
        this.fileStorage = fileStorage;
        this.metricPublisher = metricPublisher;
    }

    public Uni<Void> provisionForBrand(UUID brandId, String brandSlug, IUser registeredUser) {
        LOGGER.infof("Starting default fragment provisioning for brand %s (%s), user %s", brandSlug, brandId, registeredUser.getId());
        String sql = "SELECT id, source, status, type, title, album, length, boost, description, slug_name, expires_at " +
                "FROM " + SF_TABLE + " WHERE artist = $1 AND archived = 0";

        return client.preparedQuery(sql)
                .execute(Tuple.of(DEFAULT_ARTIST))
                .invoke(rows -> LOGGER.infof("Found %d default_mixpla fragments to provision for brand %s", rows.size(), brandSlug))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transformToUniAndConcatenate(row -> {
                    UUID originalId = row.getUUID("id");
                    return copyFragment(row, originalId, brandId, brandSlug, registeredUser)
                            .onFailure().invoke(ex -> {
                                LOGGER.errorf(ex, "Failed to provision default fragment %s for brand %s", originalId, brandId);
                                metricPublisher.publishMetric(
                                        brandSlug,
                                        MetricEventType.ERROR,
                                        ProcessType.INDEPENDENT,
                                        "default_fragment_provision_failed",
                                        Map.of("fragmentId", originalId.toString(), "brandId", brandId.toString(),
                                                "error", ex.getMessage() != null ? ex.getMessage() : "unknown")
                                );
                            })
                            .onFailure().recoverWithItem((Void) null);
                })
                .collect().asList()
                .invoke(results -> LOGGER.infof("Finished provisioning for brand %s: %d fragments processed", brandSlug, results.size()))
                .replaceWithVoid();
    }

    private Uni<Void> copyFragment(Row originalRow, UUID originalId, UUID brandId, String brandSlug, IUser registeredUser) {
        UUID newId = UUID.randomUUID();
        String newSlug = originalRow.getString("slug_name") + "-" + newId.toString().replace("-", "").substring(0, 8);
        LOGGER.infof("Copying fragment %s -> %s (slug: %s) for brand %s", originalId, newId, newSlug, brandSlug);

        return fetchAndCopyFile(originalId, brandSlug, newId)
                .onItem().transformToUni(fileInfo ->
                        doDbInserts(originalRow, originalId, newId, newSlug, brandId, brandSlug, fileInfo, registeredUser)
                );
    }

    private Uni<String[]> fetchAndCopyFile(UUID originalId, String brandSlug, UUID newId) {
        String sql = "SELECT file_key, mime_type, file_original_name, slug_name FROM _files " +
                "WHERE parent_table = $1 AND parent_id = $2 LIMIT 1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(SF_TABLE, originalId))
                .onItem().transformToUni(rows -> {
                    if (!rows.iterator().hasNext()) {
                        LOGGER.warnf("No file found for fragment %s, skipping file copy", originalId);
                        return Uni.createFrom().item((String[]) null);
                    }
                    Row fileRow = rows.iterator().next();
                    String originalKey = fileRow.getString("file_key");
                    String newKey = "music/" + brandSlug + "/default_mixpla/" + newId;
                    LOGGER.infof("Copying Hetzner file: %s -> %s", originalKey, newKey);
                    return fileStorage.copyFile(originalKey, newKey)
                            .invoke(v -> LOGGER.infof("Hetzner file copy done: %s", newKey))
                            .replaceWith(new String[]{
                                    newKey,
                                    fileRow.getString("mime_type"),
                                    fileRow.getString("file_original_name"),
                                    fileRow.getString("slug_name")
                            });
                });
    }

    private Uni<Void> doDbInserts(Row originalRow, UUID originalId, UUID newId, String newSlug,
                                   UUID brandId, String brandSlug, String[] fileInfo, IUser registeredUser) {
        OffsetDateTime now = OffsetDateTime.now();
        LOGGER.infof("Inserting DB records for new fragment %s (fileInfo present: %s)", newId, fileInfo != null);
        String insertSql = "INSERT INTO " + SF_TABLE +
                " (id, author, reg_date, last_mod_user, last_mod_date, source, status, type, " +
                "title, artist, album, length, boost, description, slug_name, expires_at) " +
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)";

        Integer boost = originalRow.getInteger("boost");
        Tuple params = Tuple.of(newId, SYSTEM_USER_ID, now, SYSTEM_USER_ID, now)
                .addString(originalRow.getString("source"))
                .addInteger(originalRow.getInteger("status"))
                .addString(originalRow.getString("type"))
                .addString(originalRow.getString("title") + " for " + brandSlug)
                .addString(DEFAULT_ARTIST)
                .addString(originalRow.getString("album"))
                .addLong(originalRow.getLong("length"))
                .addInteger(boost != null ? boost : 0)
                .addString(originalRow.getString("description"))
                .addString(newSlug)
                .addOffsetDateTime(originalRow.getOffsetDateTime("expires_at"));

        return client.withTransaction(tx ->
                tx.preparedQuery(insertSql).execute(params)
                        .invoke(v -> LOGGER.infof("Fragment row inserted: %s", newId))
                        .onItem().transformToUni(v -> insertFileRow(tx, newId, fileInfo, now))
                        .onItem().transformToUni(v -> copyAssociations(tx, "mixpla__sound_fragment_genres", "genre_id", "sound_fragment_id", originalId, newId))
                        .onItem().transformToUni(v -> copyAssociations(tx, "mixpla__sound_fragment_labels", "label_id", "id", originalId, newId))
                        .onItem().transformToUni(v -> insertRlsForUser(tx, newId, registeredUser))
                        .onItem().transformToUni(v -> insertBrandAssociation(tx, newId, brandId))
                        .invoke(v -> LOGGER.infof("All DB inserts complete for fragment %s -> brand %s", newId, brandId))
        );
    }

    private Uni<Void> insertFileRow(SqlClient tx, UUID newId, String[] fileInfo, OffsetDateTime now) {
        if (fileInfo == null) {
            return Uni.createFrom().voidItem();
        }
        String sql = "INSERT INTO _files (parent_table, parent_id, storage_type, mime_type, " +
                "file_original_name, file_key, file_bin, slug_name, reg_date, last_mod_date) " +
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)";
        return tx.preparedQuery(sql)
                .execute(Tuple.of(SF_TABLE, newId, "HETZNER", fileInfo[1], fileInfo[2], fileInfo[0])
                        .addValue(null)
                        .addString(fileInfo[3])
                        .addOffsetDateTime(now)
                        .addOffsetDateTime(now))
                .replaceWithVoid();
    }

    private Uni<Void> copyAssociations(SqlClient tx, String table, String valueCol, String ownerCol,
                                        UUID originalId, UUID newId) {
        String selectSql = "SELECT " + valueCol + " FROM " + table + " WHERE " + ownerCol + " = $1";
        String insertSql = "INSERT INTO " + table + " (" + ownerCol + ", " + valueCol + ") VALUES ($1,$2) ON CONFLICT DO NOTHING";
        return tx.preparedQuery(selectSql).execute(Tuple.of(originalId))
                .onItem().transformToUni(rows -> {
                    List<Tuple> params = new ArrayList<>();
                    rows.forEach(row -> params.add(Tuple.of(newId, row.getUUID(valueCol))));
                    if (params.isEmpty()) return Uni.createFrom().voidItem();
                    return tx.preparedQuery(insertSql).executeBatch(params).replaceWithVoid();
                });
    }

    private Uni<Void> insertRlsForUser(SqlClient tx, UUID fragmentId, IUser user) {
        String rlsSql = "INSERT INTO " + RLS_TABLE + " (reader, entity_id, can_edit, can_delete) " +
                "VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING";
        return tx.preparedQuery(rlsSql)
                .execute(Tuple.of(user.getId(), fragmentId, true, true))
                .onItem().transformToUni(v ->
                        tx.preparedQuery("SELECT id FROM _users WHERE i_su = true").execute()
                                .onItem().transformToUni(suRows -> {
                                    List<Tuple> params = new ArrayList<>();
                                    suRows.forEach(row -> params.add(Tuple.of(row.getLong("id"), fragmentId, true, true)));
                                    if (params.isEmpty()) return Uni.createFrom().voidItem();
                                    return tx.preparedQuery(rlsSql).executeBatch(params).replaceWithVoid();
                                })
                );
    }

    private Uni<Void> insertBrandAssociation(SqlClient tx, UUID fragmentId, UUID brandId) {
        String sql = "INSERT INTO mixpla__brand_sound_fragments " +
                "(brand_id, sound_fragment_id, played_by_brand_count, last_time_played_by_brand) " +
                "VALUES ($1,$2,0,NULL)";
        return tx.preparedQuery(sql).execute(Tuple.of(brandId, fragmentId)).replaceWithVoid();
    }
}
