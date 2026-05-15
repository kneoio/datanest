package com.semantyca.datanest.dto.actions;

import com.semantyca.core.dto.actions.ActionBox;
import com.semantyca.core.dto.actions.ActionsFactory;
import com.semantyca.core.model.cnst.LanguageCode;

public class SoundFragmentActionsFactory {

    public static ActionBox getViewActions() {
        return ActionsFactory.getDefaultViewActions(LanguageCode.en);
    }

}
