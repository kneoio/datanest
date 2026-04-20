package com.semantyca.datanest.dto.actions;

import com.semantyca.core.dto.actions.ActionBox;
import com.semantyca.core.dto.actions.ActionsFactory;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IRole;

import java.util.List;

public class SoundFragmentActionsFactory {

    public static ActionBox getViewActions(List<IRole> activatedRoles) {
        return ActionsFactory.getDefaultViewActions(LanguageCode.en);
    }

}
