package eu.europeana.cloud.common.model;

import org.springframework.security.acls.domain.BasePermission;

/**
 * Available permission values for eCloud resources.
 */
public enum Permission {
    READ(BasePermission.READ),
    WRITE(BasePermission.READ, BasePermission.WRITE),
    DELETE(BasePermission.DELETE),
    ADMINISTRATION(BasePermission.ADMINISTRATION),
    ALL(BasePermission.READ, BasePermission.WRITE, BasePermission.DELETE, BasePermission.ADMINISTRATION);

    private final org.springframework.security.acls.model.Permission[] permissions;

    Permission(org.springframework.security.acls.model.Permission... springPermissions) {
        this.permissions = springPermissions;
    }

    public org.springframework.security.acls.model.Permission[] getSpringPermissions() {
        return this.permissions;
    }

    public String getValue() {
        return this.name().toLowerCase();
    }

}

