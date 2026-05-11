package com.semantyca.datanest.repository.soundfragment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.datanest.model.soundfragment.SharedSoundFragment;
import com.semantyca.datanest.repository.RlsActionUtil;
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

    public Uni<List<SharedSoundFragment>> listByTargetBrandId(UUID targetBrandId) {
        String sql = "SELECT * FROM " + TABLE + " WHERE target_brand_id = $1 ORDER BY sound_fragment_id";
        return client.preparedQuery(sql)
                .execute(Tuple.of(targetBrandId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    public Uni<SharedSoundFragment> insert(SharedSoundFragment entity) {
        String insertSql = "INSERT INTO " + TABLE + " " +
                "(source_user_id, target_brand_id, sound_fragment_id, expires_at, played_count, rated_count, status, archived) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *";
        return client.withTransaction(tx ->
                tx.preparedQuery(insertSql)
                        .execute(buildInsertTuple(entity))
                        .onItem().transform(rows -> from(rows.iterator().next()))
                        .onItem().transformToUni(saved ->
                                insertRlsForShare(tx, saved.getId(), saved.getSourceUserId(), saved.getTargetBrandId())
                                        .replaceWith(saved)
                        )
        );
    }

    public Uni<SharedSoundFragment> update(UUID id, SharedSoundFragment entity) {
        String sql = "UPDATE " + TABLE + " SET " +
                "source_user_id = $1, target_brand_id = $2, sound_fragment_id = $3, expires_at = $4, " +
                "played_count = $5, rated_count = $6, status = $7, archived = $8 " +
                "WHERE id = $9";
        Tuple params = Tuple.tuple()
                .addValue(entity.getSourceUserId())
                .addUUID(entity.getTargetBrandId())
                .addUUID(entity.getSoundFragmentId())
                .addLocalDateTime(entity.getExpiresAt())
                .addInteger(entity.getPlayedCount() != null ? entity.getPlayedCount() : 0)
                .addInteger(entity.getRatedCount() != null ? entity.getRatedCount() : 0)
                .addInteger(entity.getStatus() != null ? entity.getStatus() : 1)
                .addInteger(entity.getArchived() != null ? entity.getArchived() : 0)
                .addUUID(id);
        return client.preparedQuery(sql)
                .execute(params)
                .onItem().transformToUni(rowSet -> {
                    if (rowSet.rowCount() == 0) {
                        return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                    }
                    return findById(id);
                });
    }

    public Uni<Integer> delete(UUID id) {
        String sql = "DELETE FROM " + TABLE + " WHERE id = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(id))
                .onItem().transform(SqlResult::rowCount);
    }

    public Uni<Integer> deleteBySoundFragmentAndBrand(UUID soundFragmentId, UUID targetBrandId) {
        String sql = "DELETE FROM " + TABLE + " WHERE sound_fragment_id = $1 AND target_brand_id = $2";
        return client.preparedQuery(sql)
                .execute(Tuple.of(soundFragmentId, targetBrandId))
                .onItem().transform(SqlResult::rowCount);
    }

    public Uni<Void> insertIfNotExists(SharedSoundFragment entity) {
        String insertSql = "INSERT INTO " + TABLE + " " +
                "(source_user_id, target_brand_id, sound_fragment_id, expires_at, played_count, rated_count, status, archived) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) " +
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

    private Uni<Void> insertRlsForShare(SqlClient tx, UUID entityId, Long sourceUserId, UUID targetBrandId) {
        Uni<Void> brandOwnerRls = RlsActionUtil.grantFromAuthorColumn(tx, RLS_TABLE, entityId, BRANDS_TABLE, targetBrandId, false, false);
        if (sourceUserId == null) {
            return brandOwnerRls;
        }
        Uni<Void> sourceUserRls = RlsActionUtil.grantMerge(tx, RLS_TABLE, entityId, sourceUserId, true, true);
        return Uni.combine().all().unis(sourceUserRls, brandOwnerRls).discardItems();
    }

    private Tuple buildInsertTuple(SharedSoundFragment entity) {
        return Tuple.tuple()
                .addValue(entity.getSourceUserId())
                .addUUID(entity.getTargetBrandId())
                .addUUID(entity.getSoundFragmentId())
                .addLocalDateTime(entity.getExpiresAt())
                .addInteger(entity.getPlayedCount() != null ? entity.getPlayedCount() : 0)
                .addInteger(entity.getRatedCount() != null ? entity.getRatedCount() : 0)
                .addInteger(entity.getStatus() != null ? entity.getStatus() : 500)
                .addInteger(entity.getArchived() != null ? entity.getArchived() : 0);
    }

    private SharedSoundFragment from(Row row) {
        SharedSoundFragment e = new SharedSoundFragment();
        e.setId(row.getUUID("id"));
        e.setSourceUserId(row.getLong("source_user_id"));
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
