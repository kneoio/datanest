package com.semantyca.datanest.repository.soundfragment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.datanest.model.soundfragment.SharedSoundFragment;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.SqlResult;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class SharedSoundFragmentRepository extends AsyncRepository {

    private static final String TABLE = "mixpla__shared_sound_fragments";

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
        String sql = "SELECT * FROM " + TABLE + " WHERE sound_fragment_id = $1 ORDER BY source_brand_id";
        return client.preparedQuery(sql)
                .execute(Tuple.of(soundFragmentId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    public Uni<List<SharedSoundFragment>> listBySourceBrandId(UUID sourceBrandId) {
        String sql = "SELECT * FROM " + TABLE + " WHERE source_brand_id = $1 ORDER BY sound_fragment_id";
        return client.preparedQuery(sql)
                .execute(Tuple.of(sourceBrandId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    public Uni<SharedSoundFragment> insert(SharedSoundFragment entity) {
        String sql = "INSERT INTO " + TABLE + " " +
                "(source_brand_id, sound_fragment_id, expires_at, total_played_count, total_rated_count, status, archived) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *";
        Tuple params = Tuple.tuple()
                .addUUID(entity.getSourceBrandId())
                .addUUID(entity.getSoundFragmentId())
                .addLocalDateTime(entity.getExpiresAt())
                .addInteger(entity.getTotalPlayedCount() != null ? entity.getTotalPlayedCount() : 0)
                .addInteger(entity.getTotalRatedCount() != null ? entity.getTotalRatedCount() : 0)
                .addInteger(entity.getStatus() != null ? entity.getStatus() : 1)
                .addInteger(entity.getArchived() != null ? entity.getArchived() : 0);
        return client.preparedQuery(sql)
                .execute(params)
                .onItem().transform(rows -> from(rows.iterator().next()));
    }

    public Uni<SharedSoundFragment> update(UUID id, SharedSoundFragment entity) {
        String sql = "UPDATE " + TABLE + " SET " +
                "source_brand_id = $1, sound_fragment_id = $2, expires_at = $3, " +
                "total_played_count = $4, total_rated_count = $5, status = $6, archived = $7 " +
                "WHERE id = $8";
        Tuple params = Tuple.tuple()
                .addUUID(entity.getSourceBrandId())
                .addUUID(entity.getSoundFragmentId())
                .addLocalDateTime(entity.getExpiresAt())
                .addInteger(entity.getTotalPlayedCount() != null ? entity.getTotalPlayedCount() : 0)
                .addInteger(entity.getTotalRatedCount() != null ? entity.getTotalRatedCount() : 0)
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

    /**
     * Deletes the share row for a fragment and destination brand ({@code source_brand_id}).
     */
    public Uni<Integer> deleteBySoundFragmentAndBrand(UUID soundFragmentId, UUID brandId) {
        String sql = "DELETE FROM " + TABLE + " WHERE sound_fragment_id = $1 AND source_brand_id = $2";
        return client.preparedQuery(sql)
                .execute(Tuple.of(soundFragmentId, brandId))
                .onItem().transform(SqlResult::rowCount);
    }

    /**
     * Inserts a share row if it does not already exist (unique source_brand_id + sound_fragment_id).
     */
    public Uni<Void> insertIfNotExists(SharedSoundFragment entity) {
        String sql = "INSERT INTO " + TABLE + " " +
                "(source_brand_id, sound_fragment_id, expires_at, total_played_count, total_rated_count, status, archived) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7) " +
                "ON CONFLICT ON CONSTRAINT unique_brand_shared_fragment DO NOTHING";
        Tuple params = Tuple.tuple()
                .addUUID(entity.getSourceBrandId())
                .addUUID(entity.getSoundFragmentId())
                .addLocalDateTime(entity.getExpiresAt())
                .addInteger(entity.getTotalPlayedCount() != null ? entity.getTotalPlayedCount() : 0)
                .addInteger(entity.getTotalRatedCount() != null ? entity.getTotalRatedCount() : 0)
                .addInteger(entity.getStatus() != null ? entity.getStatus() : 1)
                .addInteger(entity.getArchived() != null ? entity.getArchived() : 0);
        return client.preparedQuery(sql)
                .execute(params)
                .replaceWithVoid();
    }

    private SharedSoundFragment from(Row row) {
        SharedSoundFragment e = new SharedSoundFragment();
        e.setId(row.getUUID("id"));
        e.setSourceBrandId(row.getUUID("source_brand_id"));
        e.setSoundFragmentId(row.getUUID("sound_fragment_id"));
        e.setExpiresAt(row.getLocalDateTime("expires_at"));
        e.setTotalPlayedCount(row.getInteger("total_played_count"));
        e.setTotalRatedCount(row.getInteger("total_rated_count"));
        e.setStatus(row.getInteger("status"));
        e.setArchived(row.getInteger("archived"));
        return e;
    }
}
