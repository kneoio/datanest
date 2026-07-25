package com.semantyca.datanest.repository.soundfragment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.datanest.dto.BrandSoundFragmentFlatDTO;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import com.semantyca.mixpla.model.cnst.SourceType;
import com.semantyca.mixpla.model.filter.SoundFragmentFilter;
import com.semantyca.mixpla.model.soundfragment.SoundFragment;
import com.semantyca.officeframe.dto.GenreDTO;
import com.semantyca.officeframe.dto.LabelDTO;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class SoundFragmentBrandRepository extends SoundFragmentRepositoryAbstract {

    private final SoundFragmentQueryBuilder queryBuilder;

    @Inject
    public SoundFragmentBrandRepository(Pool client, ObjectMapper mapper, RLSRepository rlsRepository,
                                        SoundFragmentQueryBuilder queryBuilder) {
        super(client, mapper, rlsRepository);
        this.queryBuilder = queryBuilder;
    }

    public Uni<List<BrandSoundFragmentFlatDTO>> findForBrandFlat(UUID brandId, final int limit, final int offset,
                                                                  IUser user, SoundFragmentFilter filter) {
        String sql = "SELECT t.id, t.slug_name, t.title, t.artist, t.album, t.source, " +
                "bsf.played_by_brand_count, bsf.boost, bsf.last_time_played_by_brand, " +
                "EXISTS (SELECT 1 FROM mixpla__shared_sound_fragments ssf WHERE ssf.sound_fragment_id = t.id AND ssf.archived = 0) AS shared, " +
                "COALESCE(r.likes, 0) AS likes, COALESCE(r.dislikes, 0) AS dislikes";

        if (filter != null && filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty()) {
            sql += ", similarity(t.search_name, $3) AS sim";
        }

        sql += " FROM " + entityData.getTableName() + " t " +
                "JOIN mixpla__brand_sound_fragments bsf ON t.id = bsf.sound_fragment_id " +
                "JOIN " + entityData.getRlsName() + " rls ON t.id = rls.entity_id " +
                "LEFT JOIN (" +
                "  SELECT sound_fragment_id," +
                "         COUNT(*) FILTER (WHERE rating = 1)  AS likes," +
                "         COUNT(*) FILTER (WHERE rating = -1) AS dislikes" +
                "  FROM (" +
                "    SELECT DISTINCT ON (user_id, sound_fragment_id) user_id, sound_fragment_id, rating" +
                "    FROM mixpla__sound_fragment_ratings_log" +
                "    ORDER BY user_id, sound_fragment_id, created_at DESC" +
                "  ) latest" +
                "  GROUP BY sound_fragment_id" +
                ") r ON r.sound_fragment_id = t.id " +
                "WHERE bsf.brand_id = $1 AND rls.reader = $2 AND t.archived = 0";

        if (filter != null && filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty()) {
            sql += " AND (t.search_name ILIKE '%' || $3 || '%' OR similarity(t.search_name, $3) > 0.05)";
        }

        if (filter != null && filter.isActivated()) {
            sql += queryBuilder.buildFilterConditions(filter);
        }

        if (filter != null && filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty()) {
            sql += " ORDER BY sim DESC";
        } else {
            sql += " ORDER BY t.reg_date DESC";
        }

        if (limit > 0) {
            sql += String.format(" LIMIT %s OFFSET %s", limit, offset);
        }

        Tuple params = (filter != null && filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty())
                ? Tuple.of(brandId, user.getId(), filter.getSearchTerm())
                : Tuple.of(brandId, user.getId());

        return client.preparedQuery(sql)
                .execute(params)
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transformToUni(row -> createFlatDTO(row, brandId))
                .concatenate()
                .collect().asList();
    }

    public Uni<Integer> findForBrandCount(UUID brandId, IUser user, SoundFragmentFilter filter) {
        String sql = "SELECT COUNT(*) " +
                "FROM " + entityData.getTableName() + " t " +
                "JOIN mixpla__brand_sound_fragments bsf ON t.id = bsf.sound_fragment_id " +
                "JOIN " + entityData.getRlsName() + " rls ON t.id = rls.entity_id " +
                "WHERE bsf.brand_id = $1 AND rls.reader = $2 AND t.archived = 0";

        if (filter != null && filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty()) {
            sql += " AND (t.search_name ILIKE '%' || $3 || '%' OR similarity(t.search_name, $3) > 0.05)";
        }

        if (filter != null && filter.isActivated()) {
            sql += queryBuilder.buildFilterConditions(filter);
        }

        Tuple params = (filter != null && filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty())
                ? Tuple.of(brandId, user.getId(), filter.getSearchTerm())
                : Tuple.of(brandId, user.getId());

        return client.preparedQuery(sql)
                .execute(params)
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<List<SoundFragment>> getBrandSongs(UUID brandId, PlaylistItemType fragmentType, final int limit, final int offset) {
        String sql = "SELECT t.* " +
                "FROM " + entityData.getTableName() + " t " +
                "JOIN mixpla__brand_sound_fragments bsf ON t.id = bsf.sound_fragment_id " +
                "WHERE bsf.brand_id = $1 AND t.archived = 0 AND t.type = $2 " +
                "ORDER BY bsf.played_by_brand_count";

        if (limit > 0) {
            sql += String.format(" LIMIT %s OFFSET %s", limit, offset);
        }

        Tuple params = Tuple.of(brandId, fragmentType);

        return client.preparedQuery(sql)
                .execute(params)
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transformToUni(row -> from(row, true, true, true))
                .concatenate()
                .collect().asList();
    }

    private Uni<BrandSoundFragmentFlatDTO> createFlatDTO(Row row, UUID brandId) {
        UUID soundFragmentId = row.getUUID("id");

        return Uni.combine().all().unis(loadLabels(soundFragmentId), loadGenres(soundFragmentId)).asTuple()
                .onItem().transform(tuple -> {
                    BrandSoundFragmentFlatDTO dto = new BrandSoundFragmentFlatDTO();
                    dto.setSlugName(row.getString("slug_name"));
                    dto.setDefaultBrandId(brandId);
                    dto.setPlayedByBrandCount(row.getInteger("played_by_brand_count"));
                    dto.setBoost(row.getInteger("boost") != null ? row.getInteger("boost") : 0);
                    dto.setLastTimePlayedByBrand(row.getOffsetDateTime("last_time_played_by_brand"));
                    dto.setTitle(row.getString("title"));
                    dto.setArtist(row.getString("artist"));
                    dto.setAlbum(row.getString("album"));
                    dto.setSource(SourceType.valueOf(row.getString("source")));
                    dto.setLabels(tuple.getItem1());
                    dto.setGenres(tuple.getItem2());
                    dto.setShared(Boolean.TRUE.equals(row.getBoolean("shared")));
                    dto.setLikes(row.getInteger("likes"));
                    dto.setDislikes(row.getInteger("dislikes"));
                    return dto;
                });
    }

    private Uni<List<LabelDTO>> loadLabels(UUID soundFragmentId) {
        String sql = "SELECT l.id, l.identifier, l.color, l.font_color FROM __labels l " +
                "JOIN mixpla__sound_fragment_labels sfl ON l.id = sfl.label_id " +
                "WHERE sfl.id = $1 ORDER BY l.identifier";
        return client.preparedQuery(sql)
                .execute(Tuple.of(soundFragmentId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(row -> {
                    LabelDTO dto = new LabelDTO();
                    dto.setId(row.getUUID("id"));
                    dto.setIdentifier(row.getString("identifier"));
                    dto.setColor(row.getString("color"));
                    dto.setFontColor(row.getString("font_color"));
                    return dto;
                })
                .collect().asList();
    }

    private Uni<List<GenreDTO>> loadGenres(UUID soundFragmentId) {
        String sql = "SELECT g.id, g.identifier, g.color, g.font_color, g.rank FROM __genres g " +
                "JOIN mixpla__sound_fragment_genres sfg ON g.id = sfg.genre_id " +
                "WHERE sfg.sound_fragment_id = $1 ORDER BY g.identifier";
        return client.preparedQuery(sql)
                .execute(Tuple.of(soundFragmentId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(row -> {
                    GenreDTO dto = new GenreDTO();
                    dto.setId(row.getUUID("id"));
                    dto.setIdentifier(row.getString("identifier"));
                    dto.setColor(row.getString("color"));
                    dto.setFontColor(row.getString("font_color"));
                    dto.setRank(row.getInteger("rank"));
                    return dto;
                })
                .collect().asList();
    }
}
