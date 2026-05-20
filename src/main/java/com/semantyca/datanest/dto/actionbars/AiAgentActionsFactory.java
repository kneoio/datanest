package com.semantyca.datanest.dto.actionbars;

import com.semantyca.core.dto.actions.ActionBox;
import com.semantyca.core.dto.actions.ActionsFactory;
import com.semantyca.core.model.cnst.LanguageCode;

public class AiAgentActionsFactory {

    public static ActionBox getViewActions() {
        ActionBox actions = ActionsFactory.getDefaultViewActions(LanguageCode.en);
        /*Action action = new Action();
        action.setIsOn(RunMode.ON);
        action.setCaption("new_project");
        actions.addAction(action);*/
        return actions;
    }

}
