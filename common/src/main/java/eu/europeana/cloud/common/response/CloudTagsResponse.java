package eu.europeana.cloud.common.response;

import com.sun.org.apache.xpath.internal.operations.Bool;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Objects;

/**
 * Association between cloud identifier and tags of its revision
 */
@XmlRootElement
public class CloudTagsResponse implements Comparable {

    /**
     * Identifier (cloud id) of a record.
     */
    private String cloudId;

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
    public CloudTagsResponse() {
        super();
    }


    /**
     * Creates a new instance of this class.
     *
     * @param cloudId
     * @param published
     * @param deleted
     * @param acceptance
     */
    public CloudTagsResponse(String cloudId, boolean published, boolean deleted, boolean acceptance) {
        super();
        this.cloudId = cloudId;
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


    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + Objects.hashCode(this.cloudId);
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
        final CloudTagsResponse other = (CloudTagsResponse) obj;
        if (!Objects.equals(this.cloudId, other.cloudId)) {
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
        return "CloudTags{" + "cloudId=" + cloudId + ", published=" + published + ", deleted=" + deleted + ", acceptance=" + acceptance + '}';
    }

    @Override
    public int compareTo(Object o) {
        if (o == null)
            return 1;

        CloudTagsResponse other = (CloudTagsResponse) o;
        if (this.cloudId.equals(other.cloudId)) {
            if (this.published == other.published) {
                if (this.deleted == other.deleted) {
                    return Boolean.compare(this.acceptance, other.acceptance);
                }
                else {
                    return Boolean.compare(this.deleted, other.deleted);
                }
            }
            return Boolean.compare(this.published, other.published);
        }
        return this.cloudId.compareTo(other.cloudId);
    }
}
