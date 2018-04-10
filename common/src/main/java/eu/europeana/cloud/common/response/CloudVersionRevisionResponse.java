package eu.europeana.cloud.common.response;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Objects;

/**
 * Association between cloud identifier, version identifier and revision identifier.
 */
@XmlRootElement
public class CloudVersionRevisionResponse implements Comparable {

    /**
     * Identifier (cloud id) of a record.
     */
    private String cloudId;

    /**
     * Identifier of a version.
     */
    private String version;

    /**
     * Identifier of a revision.
     */
    private String revisionId;


    /**
     * Published tag
     */
    private boolean published;


    /**
     * Deleted tag
     */
    private boolean deleted;


    /**
     * Acceptance tag
     */
    private boolean acceptance;


    /**
     * Creates a new instance of this class.
     */
    public CloudVersionRevisionResponse() {
        super();
    }


    /**
     * Creates a new instance of this class.
     *
     * @param cloudId
     * @param version
     * @param revisionId
     */
    public CloudVersionRevisionResponse(String cloudId, String version, String revisionId, boolean published, boolean deleted, boolean acceptance) {
        super();
        this.cloudId = cloudId;
        this.version = version;
        this.revisionId = revisionId;
        this.published = published;
        this.deleted = deleted;
        this.acceptance = acceptance;
    }


    public String getCloudId() {
        return cloudId;
    }


    public void setCloudId(String cloudId) {
        this.cloudId = cloudId;
    }


    public String getRevisionId() {
        return revisionId;
    }


    public void setRevisionId(String revisionId) {
        this.revisionId = revisionId;
    }


    public String getVersion() {
        return version;
    }


    public void setVersion(String version) {
        this.version = version;
    }


    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + Objects.hashCode(this.cloudId);
        hash = 37 * hash + Objects.hashCode(this.version);
        hash = 37 * hash + Objects.hashCode(this.revisionId);
        hash = 37 * hash + Objects.hashCode(this.published);
        hash = 37 * hash + Objects.hashCode(this.deleted);
        hash = 37 * hash + Objects.hashCode(this.acceptance);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final CloudVersionRevisionResponse other = (CloudVersionRevisionResponse) obj;
        if (!Objects.equals(this.cloudId, other.cloudId)) {
            return false;
        }
        if (!Objects.equals(this.version, other.version)) {
            return false;
        }
        if (!Objects.equals(this.revisionId, other.revisionId)) {
            return false;
        }
        if (!Objects.equals(this.published, other.published)) {
            return false;
        }
        if (!Objects.equals(this.deleted, other.deleted)) {
            return false;
        }
        if (!Objects.equals(this.acceptance, other.acceptance)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "CloudVersionRevision{" + "cloudId=" + cloudId + ", version="
                + version + ", revisionId=" + revisionId + ", published=" + published
                + ", deleted=" + deleted + ", acceptance=" + acceptance + '}';
    }

    @Override
    public int compareTo(Object o) {
        if (o == null) {
            return 1;
        }

        CloudVersionRevisionResponse other = (CloudVersionRevisionResponse) o;
        if (this.cloudId.equals(other.cloudId)) {
            if (this.version.equals(other.version)) {
                if (this.revisionId.equals(other.revisionId)) {
                    if (Boolean.valueOf(this.published).equals(Boolean.valueOf(other.published))) {
                        if (Boolean.valueOf(this.deleted).equals(Boolean.valueOf(other.deleted)))
                            return Boolean.valueOf(this.acceptance).compareTo(Boolean.valueOf(other.acceptance));
                        return Boolean.valueOf(this.deleted).compareTo(Boolean.valueOf(other.deleted));
                    }
                    return Boolean.valueOf(this.published).compareTo(Boolean.valueOf(other.published));
                }
                return this.revisionId.compareTo(other.revisionId);
            }
            return this.version.compareTo(other.version);
        }
        return this.cloudId.compareTo(other.cloudId);
    }

    public boolean isPublished() {
        return published;
    }

    public void setPublished(boolean published) {
        this.published = published;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public boolean isAcceptance() {
        return acceptance;
    }

    public void setAcceptance(boolean acceptance) {
        this.acceptance = acceptance;
    }
}
