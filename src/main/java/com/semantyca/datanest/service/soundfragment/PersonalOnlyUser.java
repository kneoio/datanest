package com.semantyca.datanest.service.soundfragment;

import com.semantyca.core.model.user.IUser;

/**
 * Forces custom-tag label creation to always be personal to the calling user: LabelService
 * grants owner=null (shared/system) labels only to supervisors, and free-text tags typed in
 * Mixdeck must never become shared/system labels, regardless of the caller's real role.
 */
record PersonalOnlyUser(IUser delegate) implements IUser {

    @Override
    public Long getId() {
        return delegate.getId();
    }

    @Override
    public String getUserName() {
        return delegate.getUserName();
    }

    @Override
    public String getEmail() {
        return delegate.getEmail();
    }

    @Override
    public boolean isSupervisor() {
        return false;
    }

    @Override
    public void setSupervisor(boolean supervisor) {
        // no-op: this view always reports non-supervisor
    }

    @Override
    public String getLogin() {
        return delegate.getLogin();
    }

    @Override
    public java.util.TimeZone getTimeZone() {
        return delegate.getTimeZone();
    }
}
