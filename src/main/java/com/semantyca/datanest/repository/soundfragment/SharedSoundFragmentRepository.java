package com.semantyca.datanest.repository.soundfragment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.datanest.dto.MySharedContributionDTO;
import com.semantyca.datanest.dto.SharedSoundFragmentDTO;
import com.semantyca.datanest.dto.SharedSoundFragmentPreviewDTO;
import com.semantyca.datanest.model.soundfragment.SharedSoundFragment;
import com.semantyca.datanest.repository.RlsActionUtil;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.SqlClient;
import io.vertx.mutiny.sqlclient.SqlResult;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class SharedSoundFragmentRepository extends AsyncRepository {

    private static final String TABLE = "mixpla__shared_sound_fragments";
    private static final String RLS_TABLE = "mixpla__shared_sound_fragment_readers";
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
        String sql = "SELECT * FROM " + TABLE + " WHERE id = $1";
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
        String sql = "SELECT * FROM " + TABLE + " WHERE sound_fragment_id = $1 ORDER BY target_brand_id";
        return client.preparedQuery(sql)
                .execute(Tuple.of(soundFragmentId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    public Uni<Integer> deleteBySoundFragmentAndBrand(UUID soundFragmentId, UUID targetBrandId) {
        String deleteSql = "DELETE FROM " + TABLE + " WHERE sound_fragment_id = $1 AND target_brand_id = $2 RETURNING id, source_user_id";
        return client.withTransaction(tx ->
                tx.preparedQuery(deleteSql)
                        .execute(Tuple.of(soundFragmentId, targetBrandId))
                        .onItem().transformToUni(rows -> {
                            if (!rows.iterator().hasNext()) {
                                return Uni.createFrom().item(0);
                            }
                            Row row = rows.iterator().next();
                            UUID entityId = row.getUUID("id");
                            long sourceUserId = row.getLong("source_user_id");
                            return Uni.combine().all().unis(
                                    RlsActionUtil.revoke(tx, RLS_TABLE, entityId, sourceUserId),
                                    RlsActionUtil.revokeFromJsonField(tx, RLS_TABLE, entityId, BRANDS_TABLE, targetBrandId, "owner", "userId")
                            ).discardItems().replaceWith(1);
                        })
        );
    }

    public Uni<Integer> deleteByFragmentIdAndReader(UUID soundFragmentId, long userId) {
        String deleteSql = "DELETE FROM " + TABLE +
                " WHERE sound_fragment_id = $1 AND id IN " +
                "(SELECT entity_id FROM " + RLS_TABLE + " WHERE reader = $2) " +
                "RETURNING id, source_user_id, target_brand_id";
        return client.withTransaction(tx ->
                tx.preparedQuery(deleteSql)
                        .execute(Tuple.of(soundFragmentId, userId))
                        .onItem().transformToUni(rows -> {
                            if (!rows.iterator().hasNext()) {
                                return Uni.createFrom().item(0);
                            }
                            Row row = rows.iterator().next();
                            UUID entityId = row.getUUID("id");
                            long sourceUserId = row.getLong("source_user_id");
                            UUID targetBrandId = row.getUUID("target_brand_id");
                            return Uni.combine().all().unis(
                                    RlsActionUtil.revoke(tx, RLS_TABLE, entityId, sourceUserId),
                                    RlsActionUtil.revokeFromJsonField(tx, RLS_TABLE, entityId, BRANDS_TABLE, targetBrandId, "owner", "userId")
                            ).discardItems().replaceWith(1);
                        })
        );
    }

    public Uni<Void> insertIfNotExists(SharedSoundFragment entity) {
        String insertSql = "INSERT INTO " + TABLE + " " +
                "(source_user_id, target_brand_id, sound_fragment_id, expires_at, played_count, rated_count, status, archived, source_user_name, source_user_email) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) " +
                "ON CONFLICT ON CONSTRAINT unique_brand_shared_fragment DO NOTHING RETURNING id";
        return client.withTransaction(tx ->
                tx.preparedQuery(insertSql)
                        .execute(buildInsertTuple(entity))
                        .onItem().transformToUni(rows -> {
                            if (!rows.iterator().hasNext()) {
                                return Uni.createFrom().voidItem();
                            }
                            UUID newId = rows.iterator().next().getUUID("id");
                            return insertRlsForShare(tx, newId, entity.getSourceUserId(), entity.getTargetBrandId());
                        })
        );
    }

    private Uni<Void> insertRlsForShare(SqlClient tx, UUID entityId, long sourceUserId, UUID targetBrandId) {
        Uni<Void> brandOwnerRls = RlsActionUtil.grantFromJsonField(tx, RLS_TABLE, entityId, BRANDS_TABLE, targetBrandId, "owner", "userId", false, false);
        Uni<Void> sourceUserRls = RlsActionUtil.grantMerge(tx, RLS_TABLE, entityId, sourceUserId, true, true);
        return Uni.combine().all().unis(sourceUserRls, brandOwnerRls).discardItems();
    }

    private Tuple buildInsertTuple(SharedSoundFragment entity) {
        return Tuple.tuple()
                .addValue(entity.getSourceUserId())
                .addUUID(entity.getTargetBrandId())
                .addUUID(entity.getSoundFragmentId())
                .addLocalDateTime(entity.getExpiresAt())
                .addInteger(0)
                .addInteger(100)
                .addInteger(entity.getStatus())
                .addInteger(0)
                .addValue(entity.getSourceUserName())
                .addValue(entity.getSourceUserEmail());
    }

    public Uni<List<MySharedContributionDTO>> getMyContributions(int limit, int offset, long userId) {
        String sql = "SELECT sf.id, sf.title, sf.artist, sf.type, sf.album " +
                "FROM " + SF_TABLE + " sf " +
                "JOIN " + SF_RLS_TABLE + " rls ON sf.id = rls.entity_id " +
                "WHERE rls.reader = $1 AND sf.author = $2 AND sf.archived = 0 " +
                "AND EXISTS (SELECT 1 FROM " + TABLE + " ssf WHERE ssf.sound_fragment_id = sf.id) " +
                "ORDER BY sf.reg_date DESC LIMIT $3 OFFSET $4";
        return client.preparedQuery(sql)
                .execute(Tuple.of(userId, userId, limit, offset))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transformToUni(this::fromContributionRow)
                .concatenate()
                .collect().asList();
    }

    public Uni<Integer> getMyContributionsCount(long userId) {
        String sql = "SELECT COUNT(*) FROM " + SF_TABLE + " sf " +
                "JOIN " + SF_RLS_TABLE + " rls ON sf.id = rls.entity_id " +
                "WHERE rls.reader = $1 AND sf.author = $2 AND sf.archived = 0 " +
                "AND EXISTS (SELECT 1 FROM " + TABLE + " ssf WHERE ssf.sound_fragment_id = sf.id)";
        return client.preparedQuery(sql)
                .execute(Tuple.of(userId, userId))
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    private Uni<MySharedContributionDTO> fromContributionRow(Row row) {
        MySharedContributionDTO dto = new MySharedContributionDTO();
        UUID sfId = row.getUUID("id");
        dto.setId(sfId);
        dto.setTitle(row.getString("title"));
        dto.setArtist(row.getString("artist"));
        dto.setType(PlaylistItemType.valueOf(row.getString("type")));
        dto.setAlbum(row.getString("album"));
        return loadGenres(sfId).chain(genres -> {
            dto.setGenres(genres);
            return loadLabels(sfId);
        }).chain(labels -> {
            dto.setLabels(labels);
            return listBySoundFragmentId(sfId);
        }).map(shares -> {
            dto.setShares(shares.stream().map(this::toShareDTO).collect(java.util.stream.Collectors.toList()));
            return dto;
        });
    }

    private SharedSoundFragmentDTO toShareDTO(SharedSoundFragment e) {
        SharedSoundFragmentDTO dto = new SharedSoundFragmentDTO();
        dto.setId(e.getId());
        dto.setSourceUserId(e.getSourceUserId());
        dto.setSourceUserName(e.getSourceUserName());
        dto.setSourceUserEmail(e.getSourceUserEmail());
        dto.setTargetBrandId(e.getTargetBrandId());
        dto.setSoundFragmentId(e.getSoundFragmentId());
        dto.setExpiresAt(e.getExpiresAt());
        dto.setPlayedCount(e.getPlayedCount());
        dto.setRatedCount(e.getRatedCount());
        dto.setStatus(e.getStatus());
        dto.setArchived(e.getArchived());
        return dto;
    }

    public Uni<List<SharedSoundFragmentPreviewDTO>> getPreviewList(int limit, int offset, long userId) {
        String sql = "SELECT sf.id, sf.title, sf.artist, sf.type, sf.album, " +
                "ssf.source_user_name, ssf.source_user_email " +
                "FROM " + SF_TABLE + " sf " +
                "JOIN " + TABLE + " ssf ON ssf.sound_fragment_id = sf.id " +
                "JOIN " + RLS_TABLE + " rls ON rls.entity_id = ssf.id " +
                "WHERE rls.reader = $1 AND sf.archived = 0 " +
                "ORDER BY sf.reg_date DESC LIMIT $2 OFFSET $3";
        return client.preparedQuery(sql)
                .execute(Tuple.of(userId, limit, offset))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transformToUni(this::fromPreviewRow)
                .concatenate()
                .collect().asList();
    }

    public Uni<Integer> getPreviewCount(long userId) {
        String sql = "SELECT COUNT(DISTINCT sf.id) FROM " + SF_TABLE + " sf " +
                "JOIN " + TABLE + " ssf ON ssf.sound_fragment_id = sf.id " +
                "JOIN " + RLS_TABLE + " rls ON rls.entity_id = ssf.id " +
                "WHERE rls.reader = $1 AND sf.archived = 0";
        return client.preparedQuery(sql)
                .execute(Tuple.of(userId))
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<SharedSoundFragmentPreviewDTO> getPreviewById(UUID soundFragmentId, long userId) {
        String sql = "SELECT sf.id, sf.title, sf.artist, sf.type, sf.album, " +
                "ssf.source_user_name, ssf.source_user_email " +
                "FROM " + SF_TABLE + " sf " +
                "JOIN " + TABLE + " ssf ON ssf.sound_fragment_id = sf.id " +
                "JOIN " + RLS_TABLE + " rls ON rls.entity_id = ssf.id " +
                "WHERE sf.id = $1 AND rls.reader = $2 AND sf.archived = 0 LIMIT 1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(soundFragmentId, userId))
                .onItem().transformToUni(rows -> {
                    if (!rows.iterator().hasNext()) {
                        throw new DocumentHasNotFoundException(soundFragmentId);
                    }
                    return fromPreviewRow(rows.iterator().next());
                });
    }

    private Uni<SharedSoundFragmentPreviewDTO> fromPreviewRow(Row row) {
        SharedSoundFragmentPreviewDTO dto = new SharedSoundFragmentPreviewDTO();
        dto.setId(row.getUUID("id"));
        dto.setTitle(row.getString("title"));
        dto.setArtist(row.getString("artist"));
        dto.setType(PlaylistItemType.valueOf(row.getString("type")));
        dto.setAlbum(row.getString("album"));
        dto.setSourceUserName(row.getString("source_user_name"));
        dto.setSourceUserEmail(row.getString("source_user_email"));
        UUID sfId = dto.getId();
        return loadGenres(sfId).chain(genres -> {
            dto.setGenres(genres);
            return loadLabels(sfId);
        }).map(labels -> {
            dto.setLabels(labels);
            return dto;
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

    private SharedSoundFragment from(Row row) {
        SharedSoundFragment e = new SharedSoundFragment();
        e.setId(row.getUUID("id"));
        e.setSourceUserId(row.getLong("source_user_id"));
        e.setSourceUserName(row.getString("source_user_name"));
        e.setSourceUserEmail(row.getString("source_user_email"));
        e.setTargetBrandId(row.getUUID("target_brand_id"));
        e.setSoundFragmentId(row.getUUID("sound_fragment_id"));
        e.setExpiresAt(row.getLocalDateTime("expires_at"));
        e.setPlayedCount(row.getInteger("played_count"));
        e.setRatedCount(row.getInteger("rated_count"));
        e.setStatus(row.getInteger("status"));
        e.setArchived(row.getInteger("archived"));
        return e;
    }
}
