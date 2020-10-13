package eu.europeana.cloud.common.model;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;
import java.util.Objects;

/**
 * Association between cloud identifier, revision Timestamps
 */
@XmlRootElement
@JsonRootName(CloudIdAndTimestampResponse.XSI_TYPE)
public class CloudIdAndTimestampResponse {

    final static String XSI_TYPE = "cloudIdAndTimestampResponse";

    @JacksonXmlProperty(namespace = "http://www.w3.org/2001/XMLSchema-instance", localName = "type", isAttribute = true)
    private final String xsiType = XSI_TYPE;

    /**
     * Identifier (cloud id) of a record.
     */

    private String cloudId;

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


    public Date getRevisionTimestamp() {
        return revisionTimestamp;
    }

    public void setRevisionTimestamp(Date revisionTimestamp) {
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
        if ((cloudId == null) && (revisionTimestamp == null)) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "CloudIdAndTimestamp{" + "cloudId=" + cloudId + ", revisionTimestamp = "
                + revisionTimestamp + '}';
    }

}