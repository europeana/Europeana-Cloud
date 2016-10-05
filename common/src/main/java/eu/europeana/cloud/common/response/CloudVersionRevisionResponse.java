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
    public CloudVersionRevisionResponse(String cloudId, String version, String revisionId) {
        super();
        this.cloudId = cloudId;
        this.version = version;
        this.revisionId = revisionId;
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
        return true;
    }

    @Override
    public String toString() {
        return "CloudVersionRevision{" + "cloudId=" + cloudId + ", version="
                + version + ", revisionId=" + revisionId + '}';
    }

    @Override
    public int compareTo(Object o) {
        if (o == null)
            return 1;

        CloudVersionRevisionResponse other = (CloudVersionRevisionResponse) o;
        if (this.cloudId.equals(other.cloudId)) {
            if (this.version.equals(other.version))
                return this.revisionId.compareTo(other.revisionId);
            else
                return this.version.compareTo(other.version);
        }
        return this.cloudId.compareTo(other.cloudId);
    }
}
