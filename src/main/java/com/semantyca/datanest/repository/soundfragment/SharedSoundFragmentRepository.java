package com.semantyca.datanest.repository.soundfragment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.model.FileMetadata;
import com.semantyca.core.model.cnst.FileType;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.embedded.DocumentAccessInfo;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.core.repository.rls.RlsActionUtil;
import com.semantyca.core.repository.table.EntityData;

import com.semantyca.mixpla.model.cnst.ApprovalStatus;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import com.semantyca.mixpla.model.soundfragment.SharedSoundFragment;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.SqlClient;
import io.vertx.mutiny.sqlclient.SqlResult;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class SharedSoundFragmentRepository extends AsyncRepository {

    private static final Logger LOGGER = Logger.getLogger(SharedSoundFragmentRepository.class);

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

    // 42next admin browse: every non-archived share, not scoped to a reader's RLS inbox (that's
    // what /received is for) - matches the existing sender/admin archive()/findById(id) on this
    // table, which are likewise not reader-gated.
    public Uni<List<SharedSoundFragment>> getAllAdmin(int limit, int offset) {
        String sql = "SELECT * FROM " + entityData.getTableName() +
                " WHERE archived = 0 ORDER BY reg_date DESC LIMIT $1 OFFSET $2";
        return client.preparedQuery(sql)
                .execute(Tuple.of(limit, offset))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    public Uni<Integer> getAllAdminCount() {
        String sql = "SELECT COUNT(*) FROM " + entityData.getTableName() + " WHERE archived = 0";
        return client.query(sql).execute()
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
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

    // Rejecting keeps the share (and everyone's RLS reader row) intact - only status flips to
    // REJECTED - so it stays visible to the receiver as a rejected item until they explicitly
    // remove it via archiveByReceiver. Previously this also wiped every reader row on the share,
    // which made it vanish for good the instant it was rejected, with no way back to delete it.
    public Uni<Integer> rejectByReceiver(UUID shareId, long userId) {
        String selectSql = "SELECT id, sound_fragment_id, target_brand_id FROM " + entityData.getTableName() +
                " WHERE id = $1 AND archived = 0 AND id IN (SELECT entity_id FROM " + entityData.getRlsName() + " WHERE reader = $2)";
        String updateStatusSql = "UPDATE " + entityData.getTableName() + " SET status = " + ApprovalStatus.REJECTED.value() + ", last_mod_date = NOW() WHERE id = $1";
        String deleteBsfSql = "DELETE FROM mixpla__brand_sound_fragments WHERE sound_fragment_id = $1 AND brand_id = $2";
        return client.withTransaction(tx ->
                tx.preparedQuery(selectSql)
                        .execute(Tuple.of(shareId, userId))
                        .onItem().transformToUni(rows -> {
                            if (!rows.iterator().hasNext()) {
                                return Uni.createFrom().item(0);
                            }
                            var row = rows.iterator().next();
                            UUID entityId = row.getUUID("id");
                            UUID soundFragmentId = row.getUUID("sound_fragment_id");
                            UUID targetBrandId = row.getUUID("target_brand_id");
                            return tx.preparedQuery(updateStatusSql).execute(Tuple.of(entityId))
                                    .chain(() -> tx.preparedQuery(deleteBsfSql).execute(Tuple.of(soundFragmentId, targetBrandId)))
                                    .replaceWith(1);
                        })
        );
    }

    // Receiver's own "delete" of a share they already rejected - the actual removal step that
    // used to happen automatically on reject. Gated on status = REJECTED so this can't be used
    // to silently drop access to a still-PENDING or already-ACCEPTED share.
    public Uni<Integer> archiveByReceiver(UUID shareId, long userId) {
        String sql = "UPDATE " + entityData.getTableName() + " SET archived = 1, last_mod_date = NOW() " +
                "WHERE id = $1 AND status = " + ApprovalStatus.REJECTED.value() +
                " AND id IN (SELECT entity_id FROM " + entityData.getRlsName() + " WHERE reader = $2)";
        return client.preparedQuery(sql)
                .execute(Tuple.of(shareId, userId))
                .onItem().transform(SqlResult::rowCount);
    }

    public record AcceptResult(int rowsAffected, UUID soundFragmentId, UUID targetBrandId) {
        public static final AcceptResult NONE = new AcceptResult(0, null, null);
    }

    public Uni<AcceptResult> acceptByReceiver(UUID shareId, long userId) {
        String selectSql = "SELECT id, sound_fragment_id, target_brand_id FROM " + entityData.getTableName() +
                " WHERE id = $1 AND archived = 0 AND id IN (SELECT entity_id FROM " + entityData.getRlsName() + " WHERE reader = $2)";
        String updateStatusSql = "UPDATE " + entityData.getTableName() + " SET status = " + ApprovalStatus.ACCEPTED.value() + ", last_mod_date = NOW() WHERE id = $1";
        String insertBsfSql = "INSERT INTO mixpla__brand_sound_fragments " +
                "(brand_id, sound_fragment_id, played_by_brand_count, last_time_played_by_brand) " +
                "VALUES ($1, $2, 0, NULL) ON CONFLICT DO NOTHING";
        return client.withTransaction(tx ->
                tx.preparedQuery(selectSql)
                        .execute(Tuple.of(shareId, userId))
                        .onItem().transformToUni(rows -> {
                            if (!rows.iterator().hasNext()) {
                                return Uni.createFrom().item(AcceptResult.NONE);
                            }
                            var row = rows.iterator().next();
                            UUID entityId = row.getUUID("id");
                            UUID soundFragmentId = row.getUUID("sound_fragment_id");
                            UUID targetBrandId = row.getUUID("target_brand_id");
                            return tx.preparedQuery(updateStatusSql).execute(Tuple.of(entityId))
                                    .chain(() -> tx.preparedQuery(insertBsfSql).execute(Tuple.of(targetBrandId, soundFragmentId)))
                                    .chain(() -> grantFragmentRlsToBrand(tx, soundFragmentId, targetBrandId))
                                    .replaceWith(new AcceptResult(1, soundFragmentId, targetBrandId));
                        })
        );
    }

    // Pre-existing gap: accepting a share only updated the share's own status and the brand
    // association — it never granted the accepting brand owner/co-owners RLS on the underlying
    // mixpla__sound_fragments row itself, which findForBrandFlat requires to show it in the
    // regular library. Mirrors the raw-JSON owner/coOwners extraction already used in
    // BrandRepository.getAllOpenForSubmission.
    private Uni<Void> grantFragmentRlsToBrand(SqlClient tx, UUID soundFragmentId, UUID targetBrandId) {
        String ownerSql = "INSERT INTO " + SF_RLS_TABLE + " (reader, entity_id, can_edit, can_delete) " +
                "SELECT (b.owner->>'userId')::bigint, $1, true, true " +
                "FROM " + BRANDS_TABLE + " b WHERE b.id = $2 AND b.owner->>'userId' IS NOT NULL " +
                "ON CONFLICT DO NOTHING";
        String coOwnersSql = "INSERT INTO " + SF_RLS_TABLE + " (reader, entity_id, can_edit, can_delete) " +
                "SELECT (co->>'userId')::bigint, $1, true, true " +
                "FROM " + BRANDS_TABLE + " b, jsonb_array_elements(COALESCE(b.owner->'coOwners', '[]'::jsonb)) co " +
                "WHERE b.id = $2 AND co->>'userId' IS NOT NULL " +
                "ON CONFLICT DO NOTHING";
        return tx.preparedQuery(ownerSql).execute(Tuple.of(soundFragmentId, targetBrandId))
                .chain(() -> tx.preparedQuery(coOwnersSql).execute(Tuple.of(soundFragmentId, targetBrandId)))
                .replaceWithVoid();
    }

    public Uni<Integer> archive(UUID shareId) {
        String sql = "UPDATE " + entityData.getTableName() + " SET archived = 1, last_mod_date = NOW() WHERE id = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(shareId))
                .onItem().transform(SqlResult::rowCount);
    }

    // Cascades a SoundFragment soft-delete to every share pointing at it, so the sender's
    // "sharedWith" list (listBySoundFragmentId) and the receiver's inbox don't keep referencing
    // a fragment that's gone, even though both already also filter on the fragment's own archived flag.
    public Uni<Integer> archiveBySoundFragmentId(UUID soundFragmentId) {
        String sql = "UPDATE " + entityData.getTableName() + " SET archived = 1, last_mod_date = NOW() WHERE sound_fragment_id = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(soundFragmentId))
                .onItem().transform(SqlResult::rowCount);
    }

    // Cascades a SoundFragment hard-delete: without this, mixpla__shared_sound_fragments rows
    // (and their share-entity RLS rows) become permanent orphans pointing at a sound_fragment_id
    // that no longer exists.
    public Uni<Void> deleteBySoundFragmentId(UUID soundFragmentId) {
        String deleteRlsSql = "DELETE FROM " + entityData.getRlsName() +
                " WHERE entity_id IN (SELECT id FROM " + entityData.getTableName() + " WHERE sound_fragment_id = $1)";
        String deleteSharesSql = "DELETE FROM " + entityData.getTableName() + " WHERE sound_fragment_id = $1";
        return client.withTransaction(tx ->
                tx.preparedQuery(deleteRlsSql).execute(Tuple.of(soundFragmentId))
                        .chain(() -> tx.preparedQuery(deleteSharesSql).execute(Tuple.of(soundFragmentId)))
                        .replaceWithVoid());
    }

    private Uni<Void> insertInTx(SqlClient tx, SharedSoundFragment entity) {
        return insertInTxReturningId(tx, entity).replaceWithVoid();
    }

    private Uni<UUID> insertInTxReturningId(SqlClient tx, SharedSoundFragment entity) {
        String upsertSql = "INSERT INTO " + entityData.getTableName() + " " +
                "(source_user_id, target_brand_id, sound_fragment_id, expires_at, played_count, rated_count, status, archived, source_user_name, source_user_email, notify_on_play, author, last_mod_user, reg_date, last_mod_date) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW()) " +
                "ON CONFLICT ON CONSTRAINT unique_brand_shared_fragment " +
                "DO UPDATE SET archived = 0, status = EXCLUDED.status, source_user_id = EXCLUDED.source_user_id, " +
                "source_user_name = EXCLUDED.source_user_name, source_user_email = EXCLUDED.source_user_email, " +
                "notify_on_play = EXCLUDED.notify_on_play, " +
                "last_mod_user = EXCLUDED.last_mod_user, last_mod_date = NOW() " +
                "RETURNING id";
        return tx.preparedQuery(upsertSql)
                .execute(buildInsertTuple(entity))
                .onItem().transformToUni(rows -> {
                    if (!rows.iterator().hasNext()) {
                        return Uni.createFrom().nullItem();
                    }
                    UUID id = rows.iterator().next().getUUID("id");
                    return insertRlsForReceivers(tx, id, entity.getSourceUserId(), entity.getTargetBrandId())
                            .replaceWith(id);
                });
    }

    // 42next admin create: same upsert-by-natural-key as the Mixdeck bulk patch flow
    // (unique_brand_shared_fragment), just entered from a single-entity form instead of an
    // add/remove list.
    public Uni<UUID> insert(SharedSoundFragment entity) {
        return client.withTransaction(tx -> insertInTxReturningId(tx, entity));
    }

    // 42next admin edit: mutable business fields only. target_brand_id/sound_fragment_id are the
    // share's natural key (see unique_brand_shared_fragment) and the RLS reader grant in
    // insertRlsForReceivers is derived from them - changing either here would leave stale RLS
    // rows behind, so those two are set on create only. To move a share to a different brand/
    // fragment, delete and recreate it instead.
    public Uni<Integer> update(UUID id, SharedSoundFragment entity) {
        String sql = "UPDATE " + entityData.getTableName() + " SET " +
                "source_user_id = $1, source_user_name = $2, source_user_email = $3, " +
                "expires_at = $4, boost = $5, status = $6, notify_on_play = $7, " +
                "last_mod_user = $1, last_mod_date = NOW() " +
                "WHERE id = $8";
        Tuple params = Tuple.tuple()
                .addValue(entity.getSourceUserId())
                .addValue(entity.getSourceUserName())
                .addValue(entity.getSourceUserEmail())
                .addOffsetDateTime(entity.getExpiresAt())
                .addInteger(entity.getBoost() != null ? entity.getBoost() : 0)
                .addInteger(entity.getStatus())
                .addValue(Boolean.TRUE.equals(entity.getNotifyOnPlay()))
                .addUUID(id);
        return client.preparedQuery(sql)
                .execute(params)
                .onItem().transform(SqlResult::rowCount);
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

    private static final String RECEIVED_SEARCH_CLAUSE =
            " AND (sf.title ILIKE $4 OR sf.artist ILIKE $4 OR ssf.source_user_name ILIKE $4)";

    public Uni<List<SharedSoundFragment>> getReceivedList(int limit, int offset, long userId, String search) {
        String sql = "SELECT ssf.id AS ssf_id, sf.id AS sf_id, sf.title, sf.artist, sf.type, sf.album, sf.reg_date, " +
                "ssf.source_user_name, ssf.source_user_email, ssf.boost, ssf.status, ssf.notify_on_play, b.loc_name AS target_brand_name " +
                "FROM " + SF_TABLE + " sf " +
                "JOIN " + entityData.getTableName() + " ssf ON ssf.sound_fragment_id = sf.id " +
                "JOIN " + entityData.getRlsName() + " rls ON rls.entity_id = ssf.id " +
                "LEFT JOIN " + BRANDS_TABLE + " b ON b.id = ssf.target_brand_id " +
                "WHERE rls.reader = $1 AND sf.archived = 0 AND ssf.archived = 0 " +
                (search != null ? RECEIVED_SEARCH_CLAUSE : "") +
                " ORDER BY sf.reg_date DESC LIMIT $2 OFFSET $3";
        Tuple params = Tuple.of(userId, limit, offset);
        if (search != null) {
            params = params.addString("%" + search + "%");
        }
        return client.preparedQuery(sql)
                .execute(params)
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transformToUni(this::fromSoundFragmentPreviewRow)
                .concatenate()
                .collect().asList();
    }

    public Uni<Integer> getReceivedListCount(long userId, String search) {
        String sql = "SELECT COUNT(DISTINCT sf.id) FROM " + SF_TABLE + " sf " +
                "JOIN " + entityData.getTableName() + " ssf ON ssf.sound_fragment_id = sf.id " +
                "JOIN " + entityData.getRlsName() + " rls ON rls.entity_id = ssf.id " +
                "WHERE rls.reader = $1 AND sf.archived = 0 AND ssf.archived = 0" +
                (search != null ? " AND (sf.title ILIKE $2 OR sf.artist ILIKE $2 OR ssf.source_user_name ILIKE $2)" : "");
        Tuple params = Tuple.of(userId);
        if (search != null) {
            params = params.addString("%" + search + "%");
        }
        return client.preparedQuery(sql)
                .execute(params)
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<SharedSoundFragment> findById(UUID id, long userId) {
        String sql = "SELECT ssf.id AS ssf_id, sf.id AS sf_id, sf.title, sf.artist, sf.type, sf.album, sf.reg_date, " +
                "ssf.source_user_name, ssf.source_user_email, ssf.boost, ssf.status, ssf.notify_on_play, b.loc_name AS target_brand_name " +
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
                })
                .chain(this::attachPreviewFiles);
    }

    // Lets the receiver preview the audio before accepting/rejecting. Deliberately bypasses the
    // fragment's own RLS gate (not granted until accept, see SHARING_WORKFLOW.md §0) - safe here
    // because this is only reached after findById's own share-entity RLS check above already
    // passed, so the caller is already an authorized reader of this specific share.
    private Uni<SharedSoundFragment> attachPreviewFiles(SharedSoundFragment e) {
        String sql = "SELECT slug_name, file_original_name, file_type FROM _files " +
                "WHERE parent_table = '" + SF_TABLE + "' AND parent_id = $1 AND archived = 0 ORDER BY reg_date ASC";
        return client.preparedQuery(sql)
                .execute(Tuple.of(e.getSoundFragmentId()))
                .onItem().transform(rows -> {
                    List<FileMetadata> files = new ArrayList<>();
                    for (Row row : rows) {
                        FileMetadata meta = new FileMetadata();
                        meta.setSlugName(row.getString("slug_name"));
                        meta.setFileOriginalName(row.getString("file_original_name"));
                        Integer fileTypeCode = row.getInteger("file_type");
                        if (fileTypeCode != null && fileTypeCode != 0) {
                            try { meta.setFileType(FileType.fromCode(fileTypeCode)); } catch (IllegalArgumentException ignored) {}
                        }
                        files.add(meta);
                    }
                    e.setFileMetadataList(files);
                    return e;
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
        e.setBoost(row.getInteger("boost") != null ? row.getInteger("boost") : 0);
        e.setStatus(row.getInteger("status"));
        e.setNotifyOnPlay(row.getBoolean("notify_on_play"));
        e.setRegDate(row.getOffsetDateTime("reg_date").toZonedDateTime());
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

    // Grants share-entity RLS (on the SharedSoundFragment row, not the underlying fragment) to the
    // target brand's owner AND co-owners, so all of them see the offer in their "received" inbox.
    // grantFromJsonField only ever extracted the single scalar owner->>'userId' - co-owners were
    // never granted at all. Mirrors grantFragmentRlsToBrand's owner+coOwners pattern.
    private Uni<Void> insertRlsForReceivers(SqlClient tx, UUID entityId, long sourceUserId, UUID targetBrandId) {
        String rlsTable = entityData.getRlsName();
        String ownerSql = "INSERT INTO " + rlsTable + " (reader, entity_id, can_edit, can_delete) " +
                "SELECT (b.owner->>'userId')::bigint, $1, false, false " +
                "FROM " + BRANDS_TABLE + " b WHERE b.id = $2 AND b.owner->>'userId' IS NOT NULL " +
                "ON CONFLICT (reader, entity_id) DO UPDATE SET reading_time = now()";
        String coOwnersSql = "INSERT INTO " + rlsTable + " (reader, entity_id, can_edit, can_delete) " +
                "SELECT (co->>'userId')::bigint, $1, false, false " +
                "FROM " + BRANDS_TABLE + " b, jsonb_array_elements(COALESCE(b.owner->'coOwners', '[]'::jsonb)) co " +
                "WHERE b.id = $2 AND co->>'userId' IS NOT NULL " +
                "ON CONFLICT (reader, entity_id) DO UPDATE SET reading_time = now()";
        String ownerJsonSql = "SELECT b.owner FROM " + BRANDS_TABLE + " b WHERE b.id = $1";
        return tx.preparedQuery(ownerJsonSql).execute(Tuple.of(targetBrandId))
                .invoke(rows -> {
                    Object ownerJson = rows.iterator().hasNext() ? rows.iterator().next().getValue("owner") : null;
                    //LOGGER.infof("insertRlsForReceivers: entityId=%s targetBrandId=%s raw owner json=%s", entityId, targetBrandId, ownerJson);
                })
                .chain(() -> tx.preparedQuery(ownerSql).execute(Tuple.of(entityId, targetBrandId)))
                .invoke(rows -> LOGGER.infof("insertRlsForReceivers: owner reader rows inserted=%d for entityId=%s targetBrandId=%s", rows.rowCount(), entityId, targetBrandId))
                .chain(() -> tx.preparedQuery(coOwnersSql).execute(Tuple.of(entityId, targetBrandId)))
                .invoke(rows -> LOGGER.infof("insertRlsForReceivers: coOwner reader rows inserted=%d for entityId=%s targetBrandId=%s", rows.rowCount(), entityId, targetBrandId))
                .chain(() -> RlsActionUtil.ensureSuperUserAccess(tx, rlsTable, entityId));
    }

    private Tuple buildInsertTuple(SharedSoundFragment entity) {
        // author/last_mod_user are audit columns and must never be null, even when there's no
        // resolved submitter account (e.g. an anonymous contribution) — fall back to SuperUser.
        Long auditUserId = entity.getSourceUserId() != null ? entity.getSourceUserId() : SuperUser.ID;
        return Tuple.tuple()
                .addValue(entity.getSourceUserId())
                .addUUID(entity.getTargetBrandId())
                .addUUID(entity.getSoundFragmentId())
                .addOffsetDateTime(entity.getExpiresAt())
                .addInteger(0)
                .addInteger(100)
                .addInteger(entity.getStatus())
                .addInteger(0)
                .addValue(entity.getSourceUserName())
                .addValue(entity.getSourceUserEmail())
                .addValue(Boolean.TRUE.equals(entity.getNotifyOnPlay()))
                .addValue(auditUserId)
                .addValue(auditUserId);
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
        setDefaultFields(e, row);
        e.setSourceUserId(row.getLong("source_user_id"));
        e.setSourceUserName(row.getString("source_user_name"));
        e.setSourceUserEmail(row.getString("source_user_email"));
        e.setTargetBrandId(row.getUUID("target_brand_id"));
        e.setSoundFragmentId(row.getUUID("sound_fragment_id"));
        e.setExpiresAt(row.getOffsetDateTime("expires_at"));
        e.setPlayedCount(row.getInteger("played_count"));
        e.setRatedCount(row.getInteger("rated_count"));
        e.setBoost(row.getInteger("boost") != null ? row.getInteger("boost") : 0);
        e.setStatus(row.getInteger("status"));
        e.setNotifyOnPlay(row.getBoolean("notify_on_play"));
        e.setArchived(row.getInteger("archived"));
        return e;
    }

    public Uni<Void> updateBoost(UUID sharedFragmentId, int boost) {
        String sql = "UPDATE mixpla__shared_sound_fragments SET boost=$1 WHERE id=$2";
        return client.preparedQuery(sql)
                .execute(Tuple.of(boost, sharedFragmentId))
                .onItem().ignore().andContinueWithNull();
    }

    public Uni<Void> updateBoostBySoundFragmentAndBrand(UUID soundFragmentId, UUID targetBrandId, int boost) {
        String sql = "UPDATE mixpla__shared_sound_fragments SET boost=$1 WHERE sound_fragment_id=$2 AND target_brand_id=$3";
        return client.preparedQuery(sql)
                .execute(Tuple.of(boost, soundFragmentId, targetBrandId))
                .onItem().ignore().andContinueWithNull();
    }
}
