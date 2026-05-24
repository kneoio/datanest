package com.semantyca.datanest.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.core.repository.rls.RlsActionUtil;
import com.semantyca.core.repository.table.EntityData;
import com.semantyca.mixpla.model.Scene;
import com.semantyca.mixpla.model.ScenePrompt;
import com.semantyca.mixpla.model.Script;
import com.semantyca.mixpla.model.brand.Brand;
import com.semantyca.mixpla.model.brand.BrandScriptEntry;
import com.semantyca.mixpla.repository.MixplaNameResolver;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.SqlClient;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.semantyca.mixpla.repository.MixplaNameResolver.*;

@ApplicationScoped
public class BrandPubRepository extends AsyncRepository {

    private static final Logger LOGGER = Logger.getLogger(BrandPubRepository.class);
    private static final EntityData brandEntityData = MixplaNameResolver.create().getEntityNames(RADIO_STATION);
    private static final EntityData scriptEntityData = MixplaNameResolver.create().getEntityNames(SCRIPT);
    private static final EntityData sceneEntityData = MixplaNameResolver.create().getEntityNames(SCRIPT_SCENE);

    @Inject
    public BrandPubRepository(Pool client, ObjectMapper mapper, RLSRepository rlsRepository) {
        super(client, mapper, rlsRepository);
    }

    public Uni<UUID> insertBrandWithScript(Brand brand, Script script, List<Scene> scenes, List<RlsActionDTO> rlsActions, IUser user) {
        return client.withTransaction(tx ->
                insertScript(tx, script, user)
                        .chain(scriptId -> insertScenes(tx, scriptId, scenes, user).replaceWith(scriptId))
                        .chain(scriptId -> {
                            brand.setScripts(List.of(new BrandScriptEntry(scriptId, Map.of())));
                            return insertBrand(tx, brand, rlsActions, user);
                        })
        );
    }

    public Uni<UUID> updateBrandWithScript(UUID brandId, UUID scriptId, Brand brand, Script script, List<Scene> scenes, List<RlsActionDTO> rlsActions, IUser user) {
        return client.withTransaction(tx ->
                updateScript(tx, scriptId, script, user)
                        .chain(v -> replaceScenes(tx, scriptId, scenes, user))
                        .chain(v -> {
                            brand.setScripts(List.of(new BrandScriptEntry(scriptId, Map.of())));
                            return updateBrand(tx, brandId, brand, rlsActions, user);
                        })
        );
    }

    public Uni<UUID> insertScriptAndUpdateBrand(UUID brandId, Brand brand, Script script, List<Scene> scenes, List<RlsActionDTO> rlsActions, IUser user) {
        return client.withTransaction(tx ->
                insertScript(tx, script, user)
                        .chain(scriptId -> insertScenes(tx, scriptId, scenes, user).replaceWith(scriptId))
                        .chain(scriptId -> {
                            brand.setScripts(List.of(new BrandScriptEntry(scriptId, Map.of())));
                            return updateBrand(tx, brandId, brand, rlsActions, user);
                        })
        );
    }

    public Uni<UUID> archiveScriptAndUpdateBrand(UUID brandId, UUID scriptId, Brand brand, List<RlsActionDTO> rlsActions, IUser user) {
        return client.withTransaction(tx ->
                archiveScript(tx, scriptId, user)
                        .chain(v -> updateBrand(tx, brandId, brand, rlsActions, user))
        );
    }

    private Uni<UUID> insertScript(SqlClient tx, Script script, IUser user) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "INSERT INTO mixpla__scripts (author, reg_date, last_mod_user, last_mod_date, name, slug_name, description, language_tag, timing_mode, custom, color) " +
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id";
        Tuple params = Tuple.tuple()
                .addLong(user.getId())
                .addOffsetDateTime(now)
                .addLong(user.getId())
                .addOffsetDateTime(now)
                .addString(script.getName())
                .addString(script.getSlugName())
                .addString(script.getDescription())
                .addString(script.getLanguageTag().tag())
                .addString(script.getTimingMode().name())
                .addBoolean(script.isCustom())
                .addString(script.getColor());
        return tx.preparedQuery(sql)
                .execute(params)
                .map(result -> result.iterator().next().getUUID("id"))
                .chain(scriptId -> insertRLSPermissions(tx, scriptId, scriptEntityData, user).replaceWith(scriptId));
    }

    private Uni<Void> updateScript(SqlClient tx, UUID scriptId, Script script, IUser user) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "UPDATE mixpla__scripts SET name=$1, slug_name=$2, description=$3, language_tag=$4, timing_mode=$5, custom=$6, color=$7, last_mod_user=$8, last_mod_date=$9 WHERE id=$10";
        Tuple params = Tuple.tuple()
                .addString(script.getName())
                .addString(script.getSlugName())
                .addString(script.getDescription())
                .addString(script.getLanguageTag().tag())
                .addString(script.getTimingMode().name())
                .addBoolean(script.isCustom())
                .addString(script.getColor())
                .addLong(user.getId())
                .addOffsetDateTime(now)
                .addUUID(scriptId);
        return tx.preparedQuery(sql).execute(params).replaceWithVoid();
    }

    private Uni<Void> insertScenes(SqlClient tx, UUID scriptId, List<Scene> scenes, IUser user) {
        if (scenes == null || scenes.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        Uni<Void> chain = Uni.createFrom().voidItem();
        for (Scene scene : scenes) {
            chain = chain.chain(v -> insertScene(tx, scriptId, scene, user));
        }
        return chain;
    }

    private Uni<Void> insertScene(SqlClient tx, UUID scriptId, Scene scene, IUser user) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "INSERT INTO mixpla__script_scenes (author, reg_date, last_mod_user, last_mod_date, script_id, title, start_time, duration_seconds, seq_num, one_time_run, allow_jingles, allow_ads, talkativity, weekdays, stage_playlist, actions) " +
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) RETURNING id";
        Tuple params = Tuple.tuple()
                .addLong(user.getId())
                .addOffsetDateTime(now)
                .addLong(user.getId())
                .addOffsetDateTime(now)
                .addUUID(scriptId)
                .addString(scene.getTitle())
                .addJsonArray(scene.getStartTime() != null ? new JsonArray(scene.getStartTime()) : new JsonArray())
                .addInteger(scene.getDurationSeconds())
                .addInteger(scene.getSeqNum())
                .addBoolean(scene.isOneTimeRun())
                .addBoolean(scene.isAllowJingles())
                .addBoolean(scene.isAllowAds())
                .addDouble(scene.getTalkativity())
                .addArrayOfInteger(scene.getWeekdays() != null ? scene.getWeekdays().toArray(new Integer[0]) : null)
                .addJsonObject(scene.getPlaylistRequest() != null ? JsonObject.mapFrom(scene.getPlaylistRequest()) : null)
                .addJsonArray(scene.getActions() != null && !scene.getActions().isEmpty() ? new JsonArray(scene.getActions().stream().map(JsonObject::mapFrom).toList()) : new JsonArray());
        return tx.preparedQuery(sql)
                .execute(params)
                .map(result -> result.iterator().next().getUUID("id"))
                .chain(sceneId -> insertRLSPermissions(tx, sceneId, sceneEntityData, user)
                        .chain(v -> insertScenePrompts(tx, sceneId, scene.getIntroPrompts())));
    }

    private Uni<Void> replaceScenes(SqlClient tx, UUID scriptId, List<Scene> scenes, IUser user) {
        String deletePromptsSql = "DELETE FROM mixpla__script_scene_prompts WHERE script_scene_id IN (SELECT id FROM mixpla__script_scenes WHERE script_id = $1)";
        String deleteReadersSql = "DELETE FROM mixpla__script_scene_readers WHERE entity_id IN (SELECT id FROM mixpla__script_scenes WHERE script_id = $1)";
        String deleteScenesSql = "DELETE FROM mixpla__script_scenes WHERE script_id = $1";
        return tx.preparedQuery(deletePromptsSql).execute(Tuple.of(scriptId))
                .chain(v -> tx.preparedQuery(deleteReadersSql).execute(Tuple.of(scriptId)))
                .chain(v -> tx.preparedQuery(deleteScenesSql).execute(Tuple.of(scriptId)))
                .chain(v -> insertScenes(tx, scriptId, scenes, user));
    }

    private Uni<Void> insertScenePrompts(SqlClient tx, UUID sceneId, List<ScenePrompt> prompts) {
        if (prompts == null || prompts.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        List<ScenePrompt> validPrompts = prompts.stream().filter(p -> p != null && p.getPromptId() != null).toList();
        if (validPrompts.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        String insertSql = "INSERT INTO mixpla__script_scene_prompts (script_scene_id, prompt_id, rank, weight, active, mandatory) VALUES ($1, $2, $3, $4, $5, $6)";
        List<Uni<Void>> insertUnis = new ArrayList<>();
        for (int i = 0; i < validPrompts.size(); i++) {
            ScenePrompt p = validPrompts.get(i);
            Tuple params = Tuple.of(sceneId, p.getPromptId(), p.getRank() != 0 ? p.getRank() : i,
                    p.getWeight() != null ? p.getWeight() : BigDecimal.valueOf(0.5), p.isActive(), p.isMandatory());
            insertUnis.add(tx.preparedQuery(insertSql).execute(params).replaceWithVoid());
        }
        return Uni.join().all(insertUnis).andFailFast().replaceWithVoid();
    }

    private Uni<UUID> insertBrand(SqlClient tx, Brand brand, List<RlsActionDTO> rlsActions, IUser user) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "INSERT INTO mixpla__brands (author, reg_date, last_mod_user, last_mod_date, country, time_zone, managing_mode, color, loc_name, ai_overriding, profile_overriding, bit_rate, genres, slug_name, description, profile_id, ai_agent_id, one_time_stream_policy, submission_policy, messaging_policy, title_font, popularity_rate, is_temporary, public, owner) " +
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25) RETURNING id";
        return tx.preparedQuery(sql)
                .execute(buildBrandInsertTuple(brand, user, now))
                .map(result -> result.iterator().next().getUUID("id"))
                .chain(brandId -> insertRLSPermissions(tx, brandId, brandEntityData, user)
                        .chain(v -> updateBrandScripts(tx, brandId, brand.getScripts()))
                        .chain(v -> RlsActionUtil.applyRlsActions(tx, brandEntityData.getRlsName(), brandId, rlsActions))
                        .replaceWith(brandId));
    }

    private Uni<UUID> updateBrand(SqlClient tx, UUID brandId, Brand brand, List<RlsActionDTO> rlsActions, IUser user) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "UPDATE mixpla__brands SET country=$1, time_zone=$2, managing_mode=$3, color=$4, loc_name=$5, ai_overriding=$6, profile_overriding=$7, bit_rate=$8, genres=$9, slug_name=$10, description=$11, profile_id=$12, ai_agent_id=$13, one_time_stream_policy=$14::submission_policy, submission_policy=$15, messaging_policy=$16, title_font=$17, is_temporary=$18, public=$19, last_mod_user=$20, last_mod_date=$21, owner=$22, script_mode=$23 WHERE id=$24";
        return tx.preparedQuery(sql)
                .execute(buildBrandUpdateTuple(brand, user, now, brandId))
                .chain(v -> updateBrandScripts(tx, brandId, brand.getScripts()))
                .chain(v -> RlsActionUtil.applyRlsActions(tx, brandEntityData.getRlsName(), brandId, rlsActions))
                .replaceWith(brandId);
    }

    private Uni<Void> archiveScript(SqlClient tx, UUID scriptId, IUser user) {
        String sql = "UPDATE mixpla__scripts SET archived = 1, last_mod_user = $1, last_mod_date = $2 WHERE id = $3";
        return tx.preparedQuery(sql)
                .execute(Tuple.of(user.getId(), OffsetDateTime.now(), scriptId))
                .replaceWithVoid();
    }

    private Uni<Void> updateBrandScripts(SqlClient tx, UUID brandId, List<BrandScriptEntry> scripts) {
        String deleteSql = "DELETE FROM mixpla__brand_scripts WHERE brand_id = $1";
        String insertSql = "INSERT INTO mixpla__brand_scripts (brand_id, script_id, user_variables, rank) VALUES ($1, $2, $3, $4)";
        return tx.preparedQuery(deleteSql).execute(Tuple.of(brandId))
                .chain(v -> {
                    if (scripts == null || scripts.isEmpty()) {
                        return Uni.createFrom().voidItem();
                    }
                    List<Uni<Void>> insertUnis = new ArrayList<>();
                    for (int i = 0; i < scripts.size(); i++) {
                        BrandScriptEntry entry = scripts.get(i);
                        Tuple params = Tuple.tuple()
                                .addUUID(brandId)
                                .addUUID(entry.getScriptId())
                                .addJsonObject(entry.getUserVariables() != null ? JsonObject.mapFrom(entry.getUserVariables()) : new JsonObject())
                                .addInteger(i);
                        insertUnis.add(tx.preparedQuery(insertSql).execute(params).replaceWithVoid());
                    }
                    return Uni.join().all(insertUnis).andFailFast().replaceWithVoid();
                });
    }

    private Tuple buildBrandInsertTuple(Brand brand, IUser user, OffsetDateTime now) {
        return Tuple.tuple()
                .addLong(user.getId())
                .addOffsetDateTime(now)
                .addLong(user.getId())
                .addOffsetDateTime(now)
                .addString(brand.getCountry() != null ? brand.getCountry().name() : null)
                .addString(brand.getTimeZone().getId())
                .addString(brand.getManagedBy().name())
                .addString(brand.getColor())
                .addJsonObject(JsonObject.mapFrom(brand.getLocalizedName()))
                .addJsonObject(brand.getAiOverriding() != null ? JsonObject.mapFrom(brand.getAiOverriding()) : new JsonObject())
                .addJsonObject(brand.getProfileOverriding() != null ? JsonObject.mapFrom(brand.getProfileOverriding()) : new JsonObject())
                .addJsonArray(JsonArray.of(brand.getBitRate()))
                .addJsonArray(brand.getGenres() != null ? new JsonArray(brand.getGenres().stream().map(UUID::toString).toList()) : new JsonArray())
                .addString(brand.getSlugName())
                .addString(brand.getDescription())
                .addUUID(brand.getProfileId())
                .addUUID(brand.getAiAgentId())
                .addString(brand.getOneTimeStreamPolicy().name())
                .addString(brand.getSubmissionPolicy().name())
                .addString(brand.getMessagingPolicy().name())
                .addString(brand.getTitleFont())
                .addDouble(brand.getPopularityRate())
                .addInteger(brand.getIsTemporary())
                .addInteger(brand.getPublicBrand())
                .addJsonObject(brand.getOwner() != null ? JsonObject.mapFrom(brand.getOwner()) : new JsonObject());
    }

    private Tuple buildBrandUpdateTuple(Brand brand, IUser user, OffsetDateTime now, UUID brandId) {
        return Tuple.tuple()
                .addString(brand.getCountry().name())
                .addString(brand.getTimeZone().getId())
                .addString(brand.getManagedBy().name())
                .addString(brand.getColor())
                .addJsonObject(JsonObject.mapFrom(brand.getLocalizedName()))
                .addJsonObject(brand.getAiOverriding() != null ? JsonObject.mapFrom(brand.getAiOverriding()) : new JsonObject())
                .addJsonObject(brand.getProfileOverriding() != null ? JsonObject.mapFrom(brand.getProfileOverriding()) : new JsonObject())
                .addJsonArray(JsonArray.of(brand.getBitRate()))
                .addJsonArray(brand.getGenres() != null ? new JsonArray(brand.getGenres().stream().map(UUID::toString).toList()) : new JsonArray())
                .addString(brand.getSlugName())
                .addString(brand.getDescription())
                .addUUID(brand.getProfileId())
                .addUUID(brand.getAiAgentId())
                .addString(brand.getOneTimeStreamPolicy().name())
                .addString(brand.getSubmissionPolicy().name())
                .addString(brand.getMessagingPolicy().name())
                .addString(brand.getTitleFont())
                .addInteger(brand.getIsTemporary() != null ? brand.getIsTemporary() : 0)
                .addInteger(brand.getPublicBrand())
                .addLong(user.getId())
                .addOffsetDateTime(now)
                .addJsonObject(brand.getOwner() != null ? JsonObject.mapFrom(brand.getOwner()) : new JsonObject())
                .addString(brand.getScriptMode() != null ? brand.getScriptMode() : "PREDEFINED")
                .addUUID(brandId);
    }

}
