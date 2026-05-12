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
                .addInteger(0);
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
