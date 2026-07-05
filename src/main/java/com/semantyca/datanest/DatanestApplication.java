package com.semantyca.datanest;

import com.semantyca.datanest.rest.AiAgentController;
import com.semantyca.datanest.rest.ArtistController;
import com.semantyca.datanest.rest.BrandAgentStatsController;
import com.semantyca.datanest.rest.BrandController;
import com.semantyca.datanest.rest.BulkACLController;
import com.semantyca.datanest.rest.ChatSummaryController;
import com.semantyca.datanest.rest.DraftController;
import com.semantyca.datanest.rest.EventController;
import com.semantyca.datanest.rest.LabelController;
import com.semantyca.datanest.rest.ListenerController;
import com.semantyca.datanest.rest.ProfileController;
import com.semantyca.datanest.rest.PromptController;
import com.semantyca.datanest.rest.PublicSongSubmissionController;
import com.semantyca.datanest.rest.PublicSoundFragmentController;
import com.semantyca.datanest.rest.RefController;
import com.semantyca.datanest.rest.SceneController;
import com.semantyca.datanest.rest.ScriptController;
import com.semantyca.datanest.rest.SharedSoundFragmentController;
import com.semantyca.datanest.rest.SoundFragmentBulkUploadController;
import com.semantyca.datanest.rest.SoundFragmentController;
import com.semantyca.datanest.rest.UserAdController;
import io.vertx.ext.web.Router;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class DatanestApplication {

    @Inject
    ArtistController artistController;

    @Inject
    BrandController brandController;

    @Inject
    DraftController draftController;

    @Inject
    EventController eventController;

    @Inject
    ListenerController listenerController;

    @Inject
    AiAgentController aiAgentController;


    @Inject
    ProfileController profileController;

    @Inject
    PromptController promptController;

    @Inject
    SceneController sceneController;

    @Inject
    SoundFragmentController soundFragmentController;

    @Inject
    SharedSoundFragmentController sharedSoundFragmentController;

    @Inject
    SoundFragmentBulkUploadController soundFragmentBulkUploadController;

    @Inject
    PublicSoundFragmentController publicSoundFragmentController;

    @Inject
    ScriptController scriptController;

    @Inject
    RefController refController;

    @Inject
    LabelController labelController;

    @Inject
    ChatSummaryController chatSummaryController;

    @Inject
    BulkACLController bulkACLController;

    @Inject
    UserAdController userAdController;

    @Inject
    PublicSongSubmissionController publicSongSubmissionController;

    @Inject
    BrandAgentStatsController brandAgentStatsController;

    void setupRoutes(@Observes Router router) {
        artistController.setupRoutes(router);
        brandController.setupRoutes(router);
        draftController.setupRoutes(router);
        eventController.setupRoutes(router);
        listenerController.setupRoutes(router);
        profileController.setupRoutes(router);
        promptController.setupRoutes(router);
        sceneController.setupRoutes(router);
        scriptController.setupRoutes(router);
        sharedSoundFragmentController.setupRoutes(router);
        publicSoundFragmentController.setupRoutes(router);
        soundFragmentController.setupRoutes(router);
        soundFragmentBulkUploadController.setupRoutes(router);
        publicSongSubmissionController.setupRoutes(router);
        refController.setupRoutes(router);
        labelController.setupRoutes(router);
        aiAgentController.setupRoutes(router);
        chatSummaryController.setupRoutes(router);
        bulkACLController.setupRoutes(router);
        userAdController.setupRoutes(router);
        brandAgentStatsController.setupRoutes(router);
    }
}
