package com.semantyca.datanest;

import com.semantyca.datanest.rest.AiAgentController;
import com.semantyca.datanest.rest.BrandController;
import com.semantyca.datanest.rest.DraftController;
import com.semantyca.datanest.rest.EventController;
import com.semantyca.datanest.rest.ListenerController;
import com.semantyca.datanest.rest.ProfileController;
import com.semantyca.datanest.rest.PromptController;
import com.semantyca.datanest.rest.RefController;
import com.semantyca.datanest.rest.SceneController;
import com.semantyca.datanest.rest.ScriptController;
import com.semantyca.datanest.rest.SoundFragmentController;
import io.vertx.ext.web.Router;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class DatanestApplication {

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
    ScriptController scriptController;

    @Inject
    RefController refController;

    void setupRoutes(@Observes Router router) {
        brandController.setupRoutes(router);
        draftController.setupRoutes(router);
        eventController.setupRoutes(router);
        listenerController.setupRoutes(router);
        profileController.setupRoutes(router);
        promptController.setupRoutes(router);
        sceneController.setupRoutes(router);
        scriptController.setupRoutes(router);
        soundFragmentController.setupRoutes(router);
        refController.setupRoutes(router);
        aiAgentController.setupRoutes(router);
    }
}
