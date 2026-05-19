package com.semantyca.datanest.repository.soundfragment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.embedded.DocumentAccessInfo;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.core.repository.rls.RlsActionUtil;
import com.semantyca.core.repository.table.EntityData;
import com.semantyca.datanest.model.cnst.ApprovalStatus;
import com.semantyca.datanest.model.soundfragment.SharedSoundFragment;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class SharedSoundFragmentRepository extends AsyncRepository {

    private static final EntityData entityData = new EntityData(
            "mixpla__shared_sound_fragments",
            "mixpla__shared_sound_fragment_readers"
    );
    private static final String BRANDS_TABLE = "mixpla__brands";
    private static final String SF_TABLE = "mixpla__sound_fragments";
    private static final String SF_RLS_TABLE = "mixpla__sound_fragment_readers";
    private static final String SF_GENRES_TABLE = "mixpla__sound_fragment_genres";
    private static final String SF_LABELS_TABLE = "mixpla__sound_fragment_labels";

    @Inject
    public SharedSoundFragmentRepository(Pool client, ObjectMapper mapper, RLSRepository rlsRepository) {
        super(client, mapper, rlsRepository);
    }

    public Uni<SharedSoundFragment> findById(UUID id) {
        String sql = "SELECT * FROM " + entityData.getTableName() + " WHERE id = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(id))
                .onItem().transform(RowSet::iterator)
                .onItem().transform(iterator -> {
                    if (!iterator.hasNext()) {
                        throw new DocumentHasNotFoundException(id);
                    }
                    return from(iterator.next());
                });
    }

    public Uni<List<SharedSoundFragment>> listBySoundFragmentId(UUID soundFragmentId) {
        String sql = "SELECT ssf.*, b.slug_name AS brand_slug_name, b.loc_name AS brand_loc_name " +
                "FROM " + entityData.getTableName() + " ssf " +
                "LEFT JOIN " + BRANDS_TABLE + " b ON b.id = ssf.target_brand_id " +
                "WHERE ssf.sound_fragment_id = $1 AND ssf.archived = 0 ORDER BY ssf.target_brand_id";
        return client.preparedQuery(sql)
                .execute(Tuple.of(soundFragmentId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::fromWithBrand)
                .collect().asList();
    }

    public Uni<List<UUID>> hasActiveShares(List<UUID> fragmentIds) {
        if (fragmentIds.isEmpty()) return Uni.createFrom().item(List.of());
        String inClause = fragmentIds.stream()
                .map(id -> "'" + id + "'")
                .collect(java.util.stream.Collectors.joining(", "));
        String sql = "SELECT DISTINCT sound_fragment_id FROM " + entityData.getTableName() +
                " WHERE sound_fragment_id IN (" + inClause + ") AND archived = 0";
        return client.query(sql)
                .execute()
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(row -> row.getUUID("sound_fragment_id"))
                .collect().asList();
    }

    public Uni<Integer> deleteBySoundFragmentAndBrand(UUID soundFragmentId, UUID targetBrandId) {
        return client.withTransaction(tx -> deleteInTx(tx, soundFragmentId, targetBrandId));
    }

    private Uni<Integer> deleteInTx(SqlClient tx, UUID soundFragmentId, UUID targetBrandId) {
        String selectSql = "SELECT id FROM " + entityData.getTableName() +
                " WHERE sound_fragment_id = $1 AND target_brand_id = $2";
        String deleteRlsSql = "DELETE FROM " + entityData.getRlsName() + " WHERE entity_id = $1";
        String deleteMainSql = "DELETE FROM " + entityData.getTableName() + " WHERE id = $1";
        return tx.preparedQuery(selectSql)
                .execute(Tuple.of(soundFragmentId, targetBrandId))
                .onItem().transformToUni(rows -> {
                    if (!rows.iterator().hasNext()) {
                        return Uni.createFrom().item(0);
                    }
                    UUID entityId = rows.iterator().next().getUUID("id");
                    return tx.preparedQuery(deleteRlsSql).execute(Tuple.of(entityId))
                            .chain(() -> tx.preparedQuery(deleteMainSql).execute(Tuple.of(entityId)))
                            .replaceWith(1);
                });
    }

    public Uni<Integer> rejectByReceiver(UUID shareId, long userId) {
        String selectSql = "SELECT id FROM " + entityData.getTableName() +
                " WHERE id = $1 AND id IN (SELECT entity_id FROM " + entityData.getRlsName() + " WHERE reader = $2)";
        String deleteRlsSql = "DELETE FROM " + entityData.getRlsName() + " WHERE entity_id = $1";
        String updateStatusSql = "UPDATE " + entityData.getTableName() + " SET status = " + ApprovalStatus.CANCELLED.value() + ", last_mod_date = NOW() WHERE id = $1";
        return client.withTransaction(tx ->
                tx.preparedQuery(selectSql)
                        .execute(Tuple.of(shareId, userId))
                        .onItem().transformToUni(rows -> {
                            if (!rows.iterator().hasNext()) {
                                return Uni.createFrom().item(0);
                            }
                            UUID entityId = rows.iterator().next().getUUID("id");
                            return tx.preparedQuery(deleteRlsSql).execute(Tuple.of(entityId))
                                    .chain(() -> tx.preparedQuery(updateStatusSql).execute(Tuple.of(entityId)))
                                    .replaceWith(1);
                        })
        );
    }

    public Uni<Integer> archive(UUID shareId) {
        String sql = "UPDATE " + entityData.getTableName() + " SET archived = 1, last_mod_date = NOW() WHERE id = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(shareId))
                .onItem().transform(SqlResult::rowCount);
    }

    private Uni<Void> insertInTx(SqlClient tx, SharedSoundFragment entity) {
        String upsertSql = "INSERT INTO " + entityData.getTableName() + " " +
                "(source_user_id, target_brand_id, sound_fragment_id, expires_at, played_count, rated_count, status, archived, source_user_name, source_user_email, reg_date, last_mod_date) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW()) " +
                "ON CONFLICT ON CONSTRAINT unique_brand_shared_fragment " +
                "DO UPDATE SET archived = 0, status = EXCLUDED.status, source_user_id = EXCLUDED.source_user_id, " +
                "source_user_name = EXCLUDED.source_user_name, source_user_email = EXCLUDED.source_user_email, last_mod_date = NOW() " +
                "RETURNING id";
        return tx.preparedQuery(upsertSql)
                .execute(buildInsertTuple(entity))
                .onItem().transformToUni(rows -> {
                    if (!rows.iterator().hasNext()) {
                        return Uni.createFrom().voidItem();
                    }
                    UUID id = rows.iterator().next().getUUID("id");
                    return insertRlsForReceivers(tx, id, entity.getSourceUserId(), entity.getTargetBrandId());
                });
    }

    public Uni<Void> applyPatch(UUID fragmentId, List<UUID> removeTargetBrandIds, List<SharedSoundFragment> toAdd) {
        return client.withTransaction(tx -> {
            Uni<Void> chain = Uni.createFrom().voidItem();
            for (UUID brandId : removeTargetBrandIds) {
                chain = chain.chain(() -> deleteInTx(tx, fragmentId, brandId).replaceWithVoid());
            }
            for (SharedSoundFragment entity : toAdd) {
                chain = chain.chain(() -> insertInTx(tx, entity));
            }
            return chain;
        });
    }

    public Uni<List<SharedSoundFragment>> getReceivedList(int limit, int offset, long userId) {
        String sql = "SELECT ssf.id AS ssf_id, sf.id AS sf_id, sf.title, sf.artist, sf.type, sf.album, " +
                "ssf.source_user_name, ssf.source_user_email, b.loc_name AS target_brand_name " +
                "FROM " + SF_TABLE + " sf " +
                "JOIN " + entityData.getTableName() + " ssf ON ssf.sound_fragment_id = sf.id " +
                "JOIN " + entityData.getRlsName() + " rls ON rls.entity_id = ssf.id " +
                "LEFT JOIN " + BRANDS_TABLE + " b ON b.id = ssf.target_brand_id " +
                "WHERE rls.reader = $1 AND sf.archived = 0 AND ssf.archived = 0 " +
                "ORDER BY sf.reg_date DESC LIMIT $2 OFFSET $3";
        return client.preparedQuery(sql)
                .execute(Tuple.of(userId, limit, offset))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transformToUni(this::fromSoundFragmentPreviewRow)
                .concatenate()
                .collect().asList();
    }

    public Uni<Integer> getReceivedListCount(long userId) {
        String sql = "SELECT COUNT(DISTINCT sf.id) FROM " + SF_TABLE + " sf " +
                "JOIN " + entityData.getTableName() + " ssf ON ssf.sound_fragment_id = sf.id " +
                "JOIN " + entityData.getRlsName() + " rls ON rls.entity_id = ssf.id " +
                "WHERE rls.reader = $1 AND sf.archived = 0 AND ssf.archived = 0";
        return client.preparedQuery(sql)
                .execute(Tuple.of(userId))
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<SharedSoundFragment> findById(UUID id, long userId) {
        String sql = "SELECT ssf.id AS ssf_id, sf.id AS sf_id, sf.title, sf.artist, sf.type, sf.album, " +
                "ssf.source_user_name, ssf.source_user_email, b.loc_name AS target_brand_name " +
                "FROM " + SF_TABLE + " sf " +
                "JOIN " + entityData.getTableName() + " ssf ON ssf.sound_fragment_id = sf.id " +
                "JOIN " + entityData.getRlsName() + " rls ON rls.entity_id = ssf.id " +
                "LEFT JOIN " + BRANDS_TABLE + " b ON b.id = ssf.target_brand_id " +
                "WHERE ssf.id = $1 AND rls.reader = $2 AND sf.archived = 0 AND ssf.archived = 0 LIMIT 1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(id, userId))
                .onItem().transformToUni(rows -> {
                    if (!rows.iterator().hasNext()) {
                        throw new DocumentHasNotFoundException(id);
                    }
                    return fromSoundFragmentPreviewRow(rows.iterator().next());
                });
    }

    public Uni<List<DocumentAccessInfo>> getDocumentAccessInfo(UUID documentId, IUser user) {
        return getDocumentAccessInfo(documentId, entityData, user);
    }

    private Uni<SharedSoundFragment> fromSoundFragmentRow(Row row) {
        SharedSoundFragment e = new SharedSoundFragment();
        UUID sfId = row.getUUID("id");
        e.setSoundFragmentId(sfId);
        e.setTitle(row.getString("title"));
        e.setArtist(row.getString("artist"));
        e.setType(PlaylistItemType.valueOf(row.getString("type")));
        e.setAlbum(row.getString("album"));
        return loadGenres(sfId).chain(genres -> {
            e.setGenres(genres);
            return loadLabels(sfId);
        }).map(labels -> {
            e.setLabels(labels);
            return e;
        });
    }

    private Uni<SharedSoundFragment> fromSoundFragmentPreviewRow(Row row) {
        SharedSoundFragment e = new SharedSoundFragment();
        e.setId(row.getUUID("ssf_id"));
        UUID sfId = row.getUUID("sf_id");
        e.setSoundFragmentId(sfId);
        e.setTitle(row.getString("title"));
        e.setArtist(row.getString("artist"));
        e.setType(PlaylistItemType.valueOf(row.getString("type")));
        e.setAlbum(row.getString("album"));
        e.setSourceUserName(row.getString("source_user_name"));
        e.setSourceUserEmail(row.getString("source_user_email"));
        JsonObject locNameJson = row.getJsonObject("target_brand_name");
        if (locNameJson != null) {
            EnumMap<LanguageCode, String> targetBrandName = new EnumMap<>(LanguageCode.class);
            locNameJson.getMap().forEach((key, value) ->
                    targetBrandName.put(LanguageCode.valueOf(key), (String) value));
            e.setTargetBrandName(targetBrandName);
        }
        return loadGenres(sfId).chain(genres -> {
            e.setGenres(genres);
            return loadLabels(sfId);
        }).map(labels -> {
            e.setLabels(labels);
            return e;
        });
    }

    private Uni<List<UUID>> loadGenres(UUID soundFragmentId) {
        String sql = "SELECT g.id FROM __genres g " +
                "JOIN " + SF_GENRES_TABLE + " sfg ON g.id = sfg.genre_id " +
                "WHERE sfg.sound_fragment_id = $1 ORDER BY g.identifier";
        return client.preparedQuery(sql)
                .execute(Tuple.of(soundFragmentId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(r -> r.getUUID("id"))
                .collect().asList();
    }

    private Uni<List<UUID>> loadLabels(UUID soundFragmentId) {
        String sql = "SELECT label_id FROM " + SF_LABELS_TABLE + " WHERE id = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(soundFragmentId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(r -> r.getUUID("label_id"))
                .collect().asList();
    }

    private Uni<Void> insertRlsForReceivers(SqlClient tx, UUID entityId, long sourceUserId, UUID targetBrandId) {
        Uni<Void> brandOwnerRls = RlsActionUtil.grantFromJsonField(tx, entityData.getRlsName(), entityId, BRANDS_TABLE, targetBrandId, "owner", "userId", false, false);
        return brandOwnerRls
                .chain(() -> RlsActionUtil.ensureSuperUserAccess(tx, entityData.getRlsName(), entityId));
    }

    private Tuple buildInsertTuple(SharedSoundFragment entity) {
        return Tuple.tuple()
                .addValue(entity.getSourceUserId())
                .addUUID(entity.getTargetBrandId())
                .addUUID(entity.getSoundFragmentId())
                .addOffsetDateTime(entity.getExpiresAt() != null ? entity.getExpiresAt().atOffset(ZoneOffset.UTC) : null)
                .addInteger(0)
                .addInteger(100)
                .addInteger(entity.getStatus())
                .addInteger(0)
                .addValue(entity.getSourceUserName())
                .addValue(entity.getSourceUserEmail());
    }

    private SharedSoundFragment fromWithBrand(Row row) {
        SharedSoundFragment e = from(row);
        e.setBrandSlugName(row.getString("brand_slug_name"));
        JsonObject locNameJson = row.getJsonObject("brand_loc_name");
        if (locNameJson != null) {
            EnumMap<LanguageCode, String> targetBrandName = new EnumMap<>(LanguageCode.class);
            locNameJson.getMap().forEach((key, value) ->
                    targetBrandName.put(LanguageCode.valueOf(key), (String) value));
            e.setTargetBrandName(targetBrandName);
        }
        return e;
    }

    private SharedSoundFragment from(Row row) {
        SharedSoundFragment e = new SharedSoundFragment();
        e.setId(row.getUUID("id"));
        e.setSourceUserId(row.getLong("source_user_id"));
        e.setSourceUserName(row.getString("source_user_name"));
        e.setSourceUserEmail(row.getString("source_user_email"));
        e.setTargetBrandId(row.getUUID("target_brand_id"));
        e.setSoundFragmentId(row.getUUID("sound_fragment_id"));
        e.setExpiresAt(row.getOffsetDateTime("expires_at"));
        e.setPlayedCount(row.getInteger("played_count"));
        e.setRatedCount(row.getInteger("rated_count"));
        e.setStatus(row.getInteger("status"));
        e.setArchived(row.getInteger("archived"));
        e.setRegDate(row.getOffsetDateTime("reg_date"));
        e.setLastModDate(row.getOffsetDateTime("last_mod_date"));
        return e;
    }
}
