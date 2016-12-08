package eu.europeana.cloud.common.model;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;
import java.util.Objects;

/**
 * Association between cloud identifier, version identifier and revision identifier.
 */
@XmlRootElement
public class CloudIdAndTimestampResponse {

    /**
     * Identifier (cloud id) of a record.
     */

    private String cloudId;

    public Date getRevisionTimestamp() {
        return revisionTimestamp;
    }

    public void setRevisionTimestamp(Date revisionTimestamp) {
        this.revisionTimestamp = revisionTimestamp;
    }

    /**
     * revisionTimestamp.
     */

    private Date revisionTimestamp;

    /**
     * Creates a new instance of this class.
     */
    public CloudIdAndTimestampResponse() {

    }


    /**
     * Creates a new instance of this class.
     *
     * @param cloudId
     * @param revisionTimestamp
     */
    public CloudIdAndTimestampResponse(String cloudId, Date revisionTimestamp) {
        this.cloudId = cloudId;
        this.revisionTimestamp = revisionTimestamp;
    }


    public String getCloudId() {
        return cloudId;
    }


    public void setCloudId(String cloudId) {
        this.cloudId = cloudId;
    }


    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + Objects.hashCode(this.cloudId);
        hash = 37 * hash + Objects.hashCode(this.revisionTimestamp);
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
        final CloudIdAndTimestampResponse other = (CloudIdAndTimestampResponse) obj;
        if (!Objects.equals(this.cloudId, other.cloudId)) {
            return false;
        }
        if (!Objects.equals(this.revisionTimestamp, other.revisionTimestamp)) {
            return false;

        }
        return true;
    }

    public boolean isEmpty() {
        if ((cloudId == null) && (revisionTimestamp == null))
            return true;
        return false;
    }

    @Override
    public String toString() {
        return "CloudVersionRevision{" + "cloudId=" + cloudId + ", revisionTimestamp = "
                + revisionTimestamp + '}';
    }

}
