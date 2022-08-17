package eu.europeana.cloud.common.model;

/**
 * Enum describing all possible roles (authorities actually) used in eCloud
 */
public enum Role {

    ADMIN("ROLE_ADMIN"),
    EXECUTOR("ROLE_EXECUTOR"),
    USER("ROLE_USER");

    private final String name;

    Role(String name){
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
