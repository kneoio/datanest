package com.semantyca.datanest.repository.soundfragment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.FileMetadata;
import com.semantyca.core.model.cnst.FileStorageType;
import com.semantyca.core.model.cnst.FileType;
import com.semantyca.core.model.embedded.DocumentAccessInfo;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.repository.IFileStorage;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.exception.DocumentModificationAccessException;
import com.semantyca.core.repository.exception.UploadAbsenceException;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.core.repository.rls.RlsActionUtil;
import com.semantyca.core.repository.table.EntityData;
import com.semantyca.core.service.external.hetzner.HetznerStorageService;
import com.semantyca.datanest.repository.SchedulableRepository;
import com.semantyca.datanest.util.SlugHelper;
import com.semantyca.mixpla.model.filter.SoundFragmentFilter;
import com.semantyca.mixpla.model.soundfragment.SoundFragment;
import com.semantyca.mixpla.repository.MixplaNameResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.semantyca.mixpla.repository.MixplaNameResolver.RADIO_STATION;
import static com.semantyca.mixpla.repository.MixplaNameResolver.SOUND_FRAGMENT;


@ApplicationScoped
public class SoundFragmentRepository extends SoundFragmentRepositoryAbstract implements SchedulableRepository<SoundFragment> {

    private static final Logger LOGGER = Logger.getLogger(SoundFragmentRepository.class);
    private static final EntityData entityData = MixplaNameResolver.create().getEntityNames(SOUND_FRAGMENT);
    private static final EntityData brandEntityData = MixplaNameResolver.create().getEntityNames(RADIO_STATION);
    private static final String STORAGE_BRAND_SLUG_FALLBACK = "unknown";

    private final IFileStorage fileStorage;
    private final SoundFragmentFileHandler fileHandler;
    private final SoundFragmentQueryBuilder queryBuilder;
    private final SoundFragmentBrandAssociationHandler brandHandler;

    public SoundFragmentRepository() {
        super();
        this.fileStorage = null;
        this.fileHandler = null;
        this.queryBuilder = null;
        this.brandHandler = null;
    }

    @Inject
    public SoundFragmentRepository(Pool client, ObjectMapper mapper, RLSRepository rlsRepository,
                                   HetznerStorageService fileStorage, SoundFragmentFileHandler fileHandler,
                                   SoundFragmentQueryBuilder queryBuilder, SoundFragmentBrandAssociationHandler brandHandler) {
        super(client, mapper, rlsRepository);
        this.fileStorage = fileStorage;
        this.fileHandler = fileHandler;
        this.queryBuilder = queryBuilder;
        this.brandHandler = brandHandler;
    }

    public Uni<List<SoundFragment>> getAll(final int limit, final int offset, final boolean includeArchived,
                                           final IUser user, final SoundFragmentFilter filter) {
        return getAll(limit, offset, includeArchived, user, filter, null);
    }

    public Uni<List<SoundFragment>> getAll(final int limit, final int offset, final boolean includeArchived,
                                           final IUser user, final SoundFragmentFilter filter,
                                           final Integer exactArchivedCode) {
        assert queryBuilder != null;
        String sql = queryBuilder.buildGetAllQuery(entityData.getTableName(), entityData.getRlsName(),
                user, includeArchived, filter, limit, offset, exactArchivedCode);

        if (filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty()) {
            return client.preparedQuery(sql)
                    .execute(Tuple.of(filter.getSearchTerm()))
                    .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                    .onItem().transformToUni(row -> from(row, false, false, false))
                    .concatenate()
                    .collect().asList();
        }

        return client.query(sql)
                .execute()
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transformToUni(row -> from(row, false, false, false))
                .concatenate()
                .collect().asList();
    }

    public Uni<List<SoundFragment>> getAllWithoutBrandAssociation(final int limit, final int offset,
                                                                  final IUser user, final SoundFragmentFilter filter) {
        assert queryBuilder != null;
        String sql = queryBuilder.buildGetAllWithoutBrandAssociationQuery(entityData.getTableName(), entityData.getRlsName(),
                user, filter, limit, offset);

        if (filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty()) {
            return client.preparedQuery(sql)
                    .execute(Tuple.of(filter.getSearchTerm()))
                    .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                    .onItem().transformToUni(row -> from(row, false, false, false))
                    .concatenate()
                    .collect().asList();
        }

        return client.query(sql)
                .execute()
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transformToUni(row -> from(row, false, false, false))
                .concatenate()
                .collect().asList();
    }

    public Uni<Integer> getAllCount(IUser user, boolean includeArchived, SoundFragmentFilter filter) {
        return getAllCount(user, includeArchived, filter, null);
    }

    public Uni<Integer> getAllCount(IUser user, boolean includeArchived, SoundFragmentFilter filter,
                                    Integer exactArchivedCode) {
        String sql = "SELECT COUNT(*) FROM " + entityData.getTableName() + " t, " + entityData.getRlsName() + " rls " +
                "WHERE t.id = rls.entity_id AND rls.reader = " + user.getId();

        if (exactArchivedCode != null) {
            sql += " AND t.archived = " + exactArchivedCode;
        } else if (!includeArchived) {
            sql += " AND t.archived = 0";
        }

        if (filter.isActivated()) {
            assert queryBuilder != null;
            sql += queryBuilder.buildFilterConditions(filter);
        }

        if (filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty()) {
            sql += " AND (t.search_name ILIKE '%' || $1 || '%' OR similarity(t.search_name, $1) > 0.05)";
            return client.preparedQuery(sql)
                    .execute(Tuple.of(filter.getSearchTerm()))
                    .onItem().transform(rows -> rows.iterator().next().getInteger(0));
        }

        return client.query(sql)
                .execute()
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<Integer> getAllCountWithoutBrandAssociation(IUser user, SoundFragmentFilter filter) {
        String sql = "SELECT COUNT(*) FROM " + entityData.getTableName() + " t, " + entityData.getRlsName() + " rls " +
                "WHERE t.id = rls.entity_id AND rls.reader = " + user.getId() +
                " AND t.archived = 0" +
                " AND NOT EXISTS (SELECT 1 FROM mixpla__brand_sound_fragments bsf WHERE bsf.sound_fragment_id = t.id)";

        if (filter.isActivated()) {
            assert queryBuilder != null;
            sql += queryBuilder.buildFilterConditions(filter);
        }

        if (filter.getSearchTerm() != null && !filter.getSearchTerm().trim().isEmpty()) {
            sql += " AND (t.search_name ILIKE '%' || $1 || '%' OR similarity(t.search_name, $1) > 0.05)";
            return client.preparedQuery(sql)
                    .execute(Tuple.of(filter.getSearchTerm()))
                    .onItem().transform(rows -> rows.iterator().next().getInteger(0));
        }

        return client.query(sql)
                .execute()
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }


    public Uni<FileMetadata> getFileBySlugName(UUID id, String slugName, IUser user, boolean includeArchived) {
        assert fileHandler != null;
        return fileHandler.getFileBySlugName(id, slugName)
                .onFailure().recoverWithUni(ex -> {
                    markAsCorrupted(id).subscribe().with(
                            result -> LOGGER.infof("Marked file %s as corrupted", id),
                            failure -> LOGGER.error("Failed to mark file %s as corrupted", id, failure)
                    );
                    return Uni.createFrom().failure(ex);
                });
    }

    public Uni<SoundFragment> findById(UUID uuid, Long userID, boolean includeArchived, boolean includeGenres, boolean includeFiles) {
        String sql = "SELECT theTable.*, rls.*" +
                String.format(" FROM %s theTable JOIN %s rls ON theTable.id = rls.entity_id ", entityData.getTableName(), entityData.getRlsName()) +
                "WHERE rls.reader = $1 AND theTable.id = $2";
        if (!includeArchived) {
            sql += " AND theTable.archived = 0";
        }

        return client.preparedQuery(sql)
                .execute(Tuple.of(userID, uuid))
                .onItem().transform(RowSet::iterator)
                .onItem().transformToUni(iterator -> {
                    if (iterator.hasNext()) {
                        Row row = iterator.next();
                        return from(row, includeGenres, includeFiles, true);
                    } else {
                        return Uni.createFrom().failure(new DocumentHasNotFoundException(uuid));
                    }
                });
    }

    public Uni<SoundFragment> findById(UUID uuid) {
        String sql = "SELECT * FROM " + entityData.getTableName() + " WHERE id = $1";

        return client.preparedQuery(sql)
                .execute(Tuple.of(uuid))
                .onItem().transform(RowSet::iterator)
                .onItem().transformToUni(iterator -> {
                    if (iterator.hasNext()) {
                        Row row = iterator.next();
                        return from(row, false, false, false);
                    } else {
                        return Uni.createFrom().failure(new DocumentHasNotFoundException(uuid));
                    }
                });
    }

    public Uni<SoundFragment> insert(SoundFragment doc, List<UUID> representedInBrands, List<RlsActionDTO> rlsActions, IUser user) {
        OffsetDateTime nowTime = ZonedDateTime.now(ZoneOffset.UTC).toOffsetDateTime();
        final List<FileMetadata> originalFiles = doc.getFileMetadataList();

        final List<FileMetadata> filesToProcess = (originalFiles != null && !originalFiles.isEmpty())
                ? List.of(originalFiles.getFirst())
                : null;

        if (filesToProcess == null) {
            return executeInsertTransaction(doc, user, nowTime, Uni.createFrom().voidItem(), representedInBrands, rlsActions);
        }

        FileMetadata meta = filesToProcess.getFirst();
        Path filePath = meta.getFilePath();
        if (filePath == null) {
            throw new IllegalArgumentException("File metadata contains an entry with a null file path.");
        }
        if (!Files.exists(filePath)) {
            throw new UploadAbsenceException("Upload file not found at path: " + filePath);
        }

        return resolveBrandSlugForStorage(representedInBrands, null)
                .chain(brandSlug -> {
                    meta.setFileOriginalName(filePath.getFileName().toString());
                    meta.setSlugName(SlugHelper.generateSlug(doc.getArtist(), doc.getTitle()));
                    meta.setFileKey(buildStorageFileKey(brandSlug, doc.getArtist()));
                    meta.setMimeType(detectMimeType(filePath.toString()));
                    doc.setFileMetadataList(filesToProcess);

                    return executeInsertTransaction(doc, user, nowTime, Uni.createFrom().voidItem(), representedInBrands, rlsActions)
                            .onItem().transformToUni(insertedDoc -> {
                                assert fileStorage != null;
                                return fileStorage.uploadFile(
                                                meta.getFileKey(),
                                                meta.getFilePath().toString(),
                                                meta.getMimeType()
                                        )
                                        .onItem().invoke(storedKey -> LOGGER.debugf("File stored with key: %s for doc ID: %s", storedKey, insertedDoc.getId()))
                                        .onItem().transform(ignored -> insertedDoc)
                                        .onFailure().recoverWithUni(ex -> {
                                            LOGGER.error("File failed to store for doc ID: %s. DB record was created.", insertedDoc.getId(), ex);
                                            return Uni.createFrom().failure(new RuntimeException("File storage failed after sound fragment creation", ex));
                                        });
                            });
                });
    }

    public Uni<Integer> archive(UUID id, IUser user) {
        return archive(id, entityData, user);
    }

    private Uni<Void> deleteStorageFiles(UUID uuid) {
        String getKeysSql = "SELECT file_key FROM _files WHERE parent_id = $1";
        return client.preparedQuery(getKeysSql).execute(Tuple.of(uuid))
                .onItem().transformToUni(rows -> {
                    List<String> keysToDelete = new ArrayList<>();
                    rows.forEach(row -> {
                        String key = row.getString("file_key");
                        if (key != null && !key.isBlank()) {
                            keysToDelete.add(key);
                        }
                    });

                    List<Uni<Void>> deleteFileUnis = keysToDelete.stream()
                            .map(key -> {
                                        assert fileStorage != null;
                                        return fileStorage.deleteFile(key)
                                                .onFailure().recoverWithUni(e -> {
                                                    LOGGER.errorf("Failed to delete file %s from storage for SoundFragment %s. DB record deletion will proceed.", key, uuid, e);
                                                    return Uni.createFrom().voidItem();
                                                });
                                    }
                            ).collect(Collectors.toList());

                    if (deleteFileUnis.isEmpty()) {
                        return Uni.createFrom().voidItem();
                    }
                    return Uni.combine().all().unis(deleteFileUnis).discardItems();
                });
    }

    private Uni<Integer> deleteDatabaseRecords(UUID uuid) {
        return client.withTransaction(tx -> {
            String getContributionIdsSql = "SELECT id FROM mixpla__contributions WHERE sound_fragment_id = $1";
            String deleteAgreementsSql = "DELETE FROM mixpla__upload_agreements WHERE contribution_id = ANY($1)";
            String deleteContributionsSql = "DELETE FROM mixpla__contributions WHERE sound_fragment_id = $1";
            String deleteGenresSql = "DELETE FROM mixpla__sound_fragment_genres WHERE sound_fragment_id = $1";
            String deleteRlsSql = String.format("DELETE FROM %s WHERE entity_id = $1", entityData.getRlsName());
            String deleteFilesSql = "DELETE FROM _files WHERE parent_id = $1";
            String deleteDocSql = String.format("DELETE FROM %s WHERE id = $1", entityData.getTableName());

            return tx.preparedQuery(getContributionIdsSql).execute(Tuple.of(uuid))
                    .onItem().transformToUni(rows -> {
                        List<UUID> contributionIds = new ArrayList<>();
                        rows.forEach(row -> contributionIds.add(row.getUUID("id")));
                        
                        if (contributionIds.isEmpty()) {
                            return Uni.createFrom().voidItem();
                        }
                        
                        return tx.preparedQuery(deleteAgreementsSql)
                                .execute(Tuple.of(contributionIds.toArray(new UUID[0])));
                    })
                    .onItem().transformToUni(ignored -> {
                        Uni<RowSet<Row>> contributionsDelete = tx.preparedQuery(deleteContributionsSql).execute(Tuple.of(uuid));
                        Uni<RowSet<Row>> genresDelete = tx.preparedQuery(deleteGenresSql).execute(Tuple.of(uuid));
                        Uni<RowSet<Row>> rlsDelete = tx.preparedQuery(deleteRlsSql).execute(Tuple.of(uuid));
                        Uni<RowSet<Row>> filesDelete = tx.preparedQuery(deleteFilesSql).execute(Tuple.of(uuid));

                        return Uni.combine().all().unis(contributionsDelete, genresDelete, rlsDelete, filesDelete)
                                .discardItems()
                                .onItem().transformToUni(ignored2 -> tx.preparedQuery(deleteDocSql).execute(Tuple.of(uuid)))
                                .onItem().transform(RowSet::rowCount);
                    });
        });
    }

    public Uni<Integer> delete(UUID uuid, IUser user) {
        return findById(uuid, user.getId(), true, false, false)
                .onFailure(DocumentHasNotFoundException.class).recoverWithItem(() -> {
                    LOGGER.warnf("SoundFragment %s not found, may already be deleted", uuid);
                    return null;
                })
                .onItem().transformToUni(doc -> {
                    if (doc == null) {
                        return Uni.createFrom().item(0);
                    }
                    return deleteStorageFiles(uuid)
                            .onItem().transformToUni(v -> deleteDatabaseRecords(uuid));
                });
    }

    private Uni<SoundFragment> executeInsertTransaction(SoundFragment doc, IUser user, OffsetDateTime regDate,
                                                        Uni<Void> fileUploadCompletionUni, List<UUID> representedInBrands,
                                                        List<RlsActionDTO> rlsActions) {
        return fileUploadCompletionUni.onItem().transformToUni(v -> {
            String sql = String.format(
                    "INSERT INTO %s (reg_date, author, last_mod_date, last_mod_user, source, status, type, " +
                            "title, artist, artist_id, album, length, boost, description, slug_name, expires_at, scheduler) " +
                            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) RETURNING id;",
                    entityData.getTableName()
            );

            Long lengthMillis = doc.getLength() != null ? doc.getLength().toMillis() : null;

            Tuple params = Tuple.of(regDate, user.getId(), regDate, user.getId())
                    .addString(doc.getSource().name())
                    .addInteger(doc.getStatus())
                    .addString(doc.getType().name())
                    .addString(doc.getTitle())
                    .addString(doc.getArtist())
                    .addUUID(doc.getArtistId())
                    .addString(doc.getAlbum())
                    .addLong(lengthMillis)
                    .addInteger(doc.getBoost())
                    .addString(doc.getDescription())
                    .addString(doc.getSlugName())
                    .addOffsetDateTime(doc.getExpiresAt())
                    .addValue(doc.getScheduler() != null
                            ? JsonObject.of("scheduler", JsonObject.mapFrom(doc.getScheduler()))
                            : null);

            return client.withTransaction(tx -> tx.preparedQuery(sql)
                    .execute(params)
                    .onItem().transform(result -> result.iterator().next().getUUID("id"))
                    .onItem().transformToUni(id -> {
                        Uni<Void> fileMetadataUni = insertFileMetadata(tx, id, doc);
                        return fileMetadataUni
                                .onItem().transformToUni(ignored -> insertGenreAssociations(tx, id, doc.getGenres()))
                                .onItem().transformToUni(ignored -> upsertLabels(tx, id, doc.getLabels()))
                                .onItem().transformToUni(ignored -> insertRLSPermissions(tx, id, entityData, user))
                                .onItem().transformToUni(ignored -> applyRlsActions(tx, id, rlsActions))
                                .onItem().transformToUni(ignored -> {
                                    assert brandHandler != null;
                                    return brandHandler.insertBrandAssociations(tx, id, representedInBrands, user);
                                })
                                .onItem().transform(ignored -> id);
                    })
            );
        }).onItem().transformToUni(id -> findById(id, user.getId(), true, true, true));
    }

    public Uni<Void> bulkUpdateACL(UUID entityId, List<RlsActionDTO> actions, IUser user) {
        LOGGER.infof("bulkUpdateACL: entityId=%s, userId=%s, actionsCount=%s", entityId, user.getId(), actions.size());
        return rlsRepository.findById(entityData.getRlsName(), user.getId(), entityId)
                .onItem().transformToUni(permissions -> {
                    LOGGER.infof("bulkUpdateACL: permissions[0]=%s for entity=%s", permissions[0], entityId);
                    if (!permissions[0]) {
                        return Uni.createFrom().failure(new DocumentModificationAccessException(
                                "User does not have edit permission", user.getUserName(), entityId));
                    }
                    return client.withTransaction(tx -> applyRlsActions(tx, entityId, actions));
                });
    }

    public Uni<Integer> revokeMyAccess(UUID entityId, IUser user) {
        String sql = String.format(
                "DELETE FROM %s WHERE reader = $1 AND entity_id = $2",
                entityData.getRlsName()
        );
        return client.preparedQuery(sql)
                .execute(Tuple.of(user.getId(), entityId))
                .onItem().transform(SqlResult::rowCount);
    }

    private Uni<Void> applyRlsActions(SqlClient tx, UUID entityId, List<RlsActionDTO> actions) {
        return RlsActionUtil.applyRlsActions(tx, entityData.getRlsName(), entityId, actions);
    }

    private Uni<Void> insertGenreAssociations(SqlClient tx, UUID soundFragmentId, List<UUID> genreIds) {
        if (genreIds == null || genreIds.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        String insertSql = "INSERT INTO mixpla__sound_fragment_genres (sound_fragment_id, genre_id) VALUES ($1, $2)";
        List<Tuple> params = genreIds.stream()
                .map(id -> Tuple.of(soundFragmentId, id))
                .collect(Collectors.toList());

        return tx.preparedQuery(insertSql)
                .executeBatch(params)
                .onItem().ignore().andContinueWithNull();
    }

    private Uni<Void> updateGenreAssociations(SqlClient tx, UUID soundFragmentId, List<UUID> genreIds) {
        String deleteSql = "DELETE FROM mixpla__sound_fragment_genres WHERE sound_fragment_id = $1";
        return tx.preparedQuery(deleteSql)
                .execute(Tuple.of(soundFragmentId))
                .onItem().transformToUni(ignored -> insertGenreAssociations(tx, soundFragmentId, genreIds));
    }

    private Uni<Void> insertFileMetadata(SqlClient tx, UUID id, SoundFragment doc) {
        if (doc.getFileMetadataList() == null || doc.getFileMetadataList().isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        OffsetDateTime nowTime = ZonedDateTime.now(ZoneOffset.UTC).toOffsetDateTime();
        String filesSql = "INSERT INTO _files (parent_table, parent_id, storage_type, " +
                "mime_type, file_type, file_original_name, file_key, file_bin, slug_name, reg_date, last_mod_date) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        List<Tuple> filesParams = doc.getFileMetadataList().stream()
                .map(meta -> Tuple.of(
                                        entityData.getTableName(),
                                        id,
                                        FileStorageType.HETZNER,
                                        meta.getMimeType(),
                                        meta.getFileType() != null ? meta.getFileType().getCode() : FileType.SOUND_FRAGMENT.getCode()
                                )
                                .addString(meta.getFileOriginalName())
                                .addString(meta.getFileKey())
                                .addValue(meta.getFileBin())
                                .addValue(meta.getSlugName())
                                .addValue(nowTime)
                                .addValue(nowTime)
                ).collect(Collectors.toList());

        return tx.preparedQuery(filesSql).executeBatch(filesParams).onItem().ignore().andContinueWithNull();
    }

    private Uni<Void> upsertLabels(SqlClient tx, UUID fragmentId, List<UUID> labels) {
        if (labels == null || labels.isEmpty()) {
            return tx.preparedQuery("DELETE FROM mixpla__sound_fragment_labels WHERE id = $1")
                    .execute(Tuple.of(fragmentId))
                    .replaceWithVoid();
        }

        String deleteSql = "DELETE FROM mixpla__sound_fragment_labels WHERE id = $1";
        String insertSql = "INSERT INTO mixpla__sound_fragment_labels (id, label_id) VALUES ($1, $2) ON CONFLICT DO NOTHING";

        return tx.preparedQuery(deleteSql)
                .execute(Tuple.of(fragmentId))
                .chain(() -> Multi.createFrom().iterable(labels)
                        .onItem().transformToUni(labelId ->
                                tx.preparedQuery(insertSql).execute(Tuple.of(fragmentId, labelId))
                        )
                        .merge()
                        .collect().asList()
                        .replaceWithVoid());
    }

    public Uni<List<UUID>> getBrandsForSoundFragment(UUID soundFragmentId, IUser user) {
        String sql = "SELECT bsf.brand_id " +
                "FROM mixpla__brand_sound_fragments bsf " +
                "JOIN " + entityData.getRlsName() + " rls ON bsf.sound_fragment_id = rls.entity_id " +
                "WHERE bsf.sound_fragment_id = $1 AND rls.reader = $2";

        return client.preparedQuery(sql)
                .execute(Tuple.of(soundFragmentId, user.getId()))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(row -> row.getUUID("brand_id"))
                .collect().asList();
    }

    public Uni<SoundFragment> update(UUID id, SoundFragment doc, List<UUID> representedInBrands, List<RlsActionDTO> rlsActions, IUser user) {
        return rlsRepository.findById(entityData.getRlsName(), user.getId(), id)
                .onItem().transformToUni(permissions -> {
                    if (!permissions[0]) {
                        return Uni.createFrom().failure(new DocumentModificationAccessException("User does not have edit permission", user.getUserName(), id));
                    }

                    return findById(id, user.getId(), true, true, true)
                            .onItem().transformToUni(existingDoc -> {
                                final List<FileMetadata> originalFiles = doc.getFileMetadataList();
                                final List<FileMetadata> newFiles = (originalFiles != null && !originalFiles.isEmpty())
                                        ? List.of(originalFiles.getFirst())
                                        : null;

                                Uni<Void> fileStoredUni = handleFileUpdate(id, doc, newFiles, representedInBrands);

                                return fileStoredUni.onItem().transformToUni(ignored -> {
                                    OffsetDateTime nowTime = ZonedDateTime.now(ZoneOffset.UTC).toOffsetDateTime();

                                    return client.withTransaction(tx -> {
                                        Uni<Void> chain = Uni.createFrom().voidItem();
                                        if (newFiles != null) {
                                            chain = deleteExistingFiles(tx, id)
                                                    .onItem().transformToUni(v -> insertNewFiles(tx, id, newFiles));
                                        }
                                        return chain
                                                .onItem().transformToUni(v -> updateGenreAssociations(tx, id, doc.getGenres()))
                                                .onItem().transformToUni(v -> upsertLabels(tx, id, doc.getLabels()))
                                                .onItem().transformToUni(v -> {
                                                    assert brandHandler != null;
                                                    return brandHandler.updateBrandAssociations(tx, id, representedInBrands, user);
                                                })
                                                .onItem().transformToUni(v -> applyRlsActions(tx, id, rlsActions))
                                                .onItem().transformToUni(v -> updateSoundFragmentRecord(tx, id, doc, user, nowTime));
                                    }).onItem().transformToUni(rowSet -> {
                                        if (rowSet.rowCount() == 0) {
                                            return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                                        }
                                        return findById(id, user.getId(), true, true, true);
                                    });
                                });
                            });
                });
    }

    private Uni<Void> handleFileUpdate(UUID id, SoundFragment doc, List<FileMetadata> newFiles, List<UUID> representedInBrands) {
        if (newFiles == null) {
            return Uni.createFrom().voidItem();
        }

        FileMetadata meta = newFiles.getFirst();
        if (meta.getFilePath() == null) {
            return Uni.createFrom().voidItem();
        }

        String localPath = meta.getFilePath().toString();
        Path path = Paths.get(localPath);
        if (!Files.exists(path)) {
            return Uni.createFrom().failure(new UploadAbsenceException("Upload file not found at path: " + localPath));
        }

        return resolveBrandSlugForStorage(representedInBrands, id)
                .chain(brandSlug -> {
                    String doKey = buildStorageFileKey(brandSlug, doc.getArtist());
                    meta.setFileKey(doKey);
                    meta.setMimeType(detectMimeType(localPath));
                    meta.setFileOriginalName(path.getFileName().toString());
                    meta.setSlugName(SlugHelper.generateSlug(doc.getArtist(), doc.getTitle()));

                    assert fileStorage != null;
                    return fileStorage.uploadFile(doKey, localPath, meta.getMimeType())
                            .onItem().invoke(storedKey -> LOGGER.debugf("File stored with key: %s for doc ID: %s", storedKey, id))
                            .onFailure().invoke(ex -> LOGGER.error("Failed to store file with key: %s", doKey, ex))
                            .onItem().ignore().andContinueWithNull();
                });
    }

    private static String buildStorageFileKey(String brandSlug, String artist) {
        return SlugHelper.generateSlugPath("music", brandSlug, artist, String.valueOf(UUID.randomUUID()));
    }

    private Uni<String> resolveBrandSlugForStorage(List<UUID> representedInBrands, UUID soundFragmentId) {
        if (representedInBrands != null && !representedInBrands.isEmpty()) {
            return loadBrandSlug(representedInBrands.getFirst());
        }
        if (soundFragmentId != null) {
            String sql = "SELECT b.slug_name FROM mixpla__brand_sound_fragments bsf " +
                    "JOIN " + brandEntityData.getTableName() + " b ON b.id = bsf.brand_id " +
                    "WHERE bsf.sound_fragment_id = $1 ORDER BY bsf.brand_id LIMIT 1";
            return client.preparedQuery(sql)
                    .execute(Tuple.of(soundFragmentId))
                    .onItem().transform(rows -> {
                        if (rows.iterator().hasNext()) {
                            return normalizeBrandSlug(rows.iterator().next().getString("slug_name"));
                        }
                        return STORAGE_BRAND_SLUG_FALLBACK;
                    });
        }
        return Uni.createFrom().item(STORAGE_BRAND_SLUG_FALLBACK);
    }

    private Uni<String> loadBrandSlug(UUID brandId) {
        String sql = "SELECT slug_name FROM " + brandEntityData.getTableName() + " WHERE id = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(brandId))
                .onItem().transform(rows -> {
                    if (rows.iterator().hasNext()) {
                        return normalizeBrandSlug(rows.iterator().next().getString("slug_name"));
                    }
                    return STORAGE_BRAND_SLUG_FALLBACK;
                });
    }

    private static String normalizeBrandSlug(String slugName) {
        if (slugName == null || slugName.isBlank()) {
            return STORAGE_BRAND_SLUG_FALLBACK;
        }
        return slugName;
    }

    private Uni<Void> deleteExistingFiles(SqlClient tx, UUID id) {
        String deleteSql = String.format("DELETE FROM _files WHERE parent_id = $1 AND parent_table = '%s'", entityData.getTableName());
        return tx.preparedQuery(deleteSql).execute(Tuple.of(id)).onItem().ignore().andContinueWithNull();
    }

    private Uni<Void> insertNewFiles(SqlClient tx, UUID id, List<FileMetadata> newFiles) {
        if (newFiles == null) {
            return Uni.createFrom().voidItem();
        }

        OffsetDateTime nowTime = ZonedDateTime.now(ZoneOffset.UTC).toOffsetDateTime();
        String filesSql = "INSERT INTO _files (parent_table, parent_id, storage_type, " +
                "mime_type, file_type, file_original_name, file_key, file_bin, slug_name, reg_date, last_mod_date) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        FileMetadata meta = newFiles.getFirst();
        Tuple fileParams = Tuple.of(
                        entityData.getTableName(),
                        id,
                        FileStorageType.HETZNER,
                        meta.getMimeType(),
                        meta.getFileType() != null ? meta.getFileType().getCode() : FileType.SOUND_FRAGMENT.getCode()
                )
                .addString(meta.getFileOriginalName())
                .addString(meta.getFileKey())
                .addValue(meta.getFileBin())
                .addValue(meta.getSlugName())
                .addValue(nowTime)
                .addValue(nowTime);

        return tx.preparedQuery(filesSql).execute(fileParams).onItem().ignore().andContinueWithNull();
    }

    public Uni<Void> insertEncodedFile(UUID fragmentId, FileMetadata meta) {
        OffsetDateTime nowTime = ZonedDateTime.now(ZoneOffset.UTC).toOffsetDateTime();
        String sql = "INSERT INTO _files (parent_table, parent_id, storage_type, " +
                "mime_type, file_type, file_original_name, file_key, slug_name, reg_date, last_mod_date) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        Tuple params = Tuple.of(
                        entityData.getTableName(),
                        fragmentId,
                        FileStorageType.HETZNER,
                        meta.getMimeType(),
                        meta.getFileType() != null ? meta.getFileType().getCode() : FileType.OPUS_ENCODED_SOUND_FRAGMENT.getCode()
                )
                .addString(meta.getFileOriginalName())
                .addString(meta.getFileKey())
                .addValue(meta.getSlugName())
                .addValue(nowTime)
                .addValue(nowTime);
        return client.preparedQuery(sql).execute(params).onItem().ignore().andContinueWithNull();
    }

    private Uni<RowSet<Row>> updateSoundFragmentRecord(SqlClient tx, UUID id, SoundFragment doc, IUser user, OffsetDateTime nowTime) {
        String updateSql = String.format("UPDATE %s SET last_mod_user=$1, last_mod_date=$2, " +
                        "status=$3, type=$4, title=$5, " +
                        "artist=$6, artist_id=$7, album=$8, length=$9, boost=$10, description=$11, slug_name=$12, expires_at=$13, scheduler=$14 WHERE id=$15;",
                entityData.getTableName());

        Tuple params = Tuple.of(user.getId(), nowTime)
                .addInteger(doc.getStatus())
                .addString(doc.getType().name())
                .addString(doc.getTitle())
                .addString(doc.getArtist())
                .addUUID(doc.getArtistId())
                .addString(doc.getAlbum())
                .addLong(doc.getLength() != null ? doc.getLength().toMillis() : null)
                .addInteger(doc.getBoost())
                .addString(doc.getDescription())
                .addString(doc.getSlugName())
                .addOffsetDateTime(doc.getExpiresAt())
                .addValue(doc.getScheduler() != null
                        ? JsonObject.of("scheduler", JsonObject.mapFrom(doc.getScheduler()))
                        : null)
                .addUUID(id);

        return tx.preparedQuery(updateSql).execute(params);
    }

    @Override
    public Uni<List<SoundFragment>> findActiveScheduled() {
        String sql = "SELECT t.* FROM " + entityData.getTableName() + " t " +
                "JOIN " + entityData.getRlsName() + " rls ON t.id = rls.entity_id " +
                "WHERE t.archived = 0 AND t.scheduler IS NOT NULL AND rls.reader = $1";

        return client.preparedQuery(sql)
                .execute(Tuple.of(SuperUser.build().getId()))
                .onFailure().invoke(throwable -> LOGGER.error("Failed to retrieve active scheduled sound fragments", throwable))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transformToUni(row -> from(row, false, false, false))
                .concatenate()
                .select().where(sf -> sf.getScheduler() != null && sf.getScheduler().isEnabled())
                .collect().asList();
    }

    public Uni<List<DocumentAccessInfo>> getDocumentAccessInfo(UUID documentId, IUser user) {
        return getDocumentAccessInfo(documentId, entityData, user);
    }
}