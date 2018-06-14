package eu.europeana.cloud.common.model;

import eu.europeana.cloud.common.utils.DateAdapter;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.io.Serializable;
import java.util.Date;

/**
 * Created by Tarek on 8/1/2016.
 */

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Revision implements Serializable {
    private String revisionName;
    private String revisionProviderId;

    @XmlElement(name = "creationTimeStamp", required = true)
    @XmlJavaTypeAdapter(DateAdapter.class)
    private Date creationTimeStamp;

    boolean published;
    boolean acceptance;
    boolean deleted;

    public Date getCreationTimeStamp() {
        return creationTimeStamp;
    }

    public void setCreationTimeStamp(Date creationTimeStamp) {
        this.creationTimeStamp = creationTimeStamp;
    }

    public boolean isAcceptance() {
        return acceptance;
    }

    public void setAcceptance(boolean acceptance) {
        this.acceptance = acceptance;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public boolean isPublished() {
        return published;
    }

    public void setPublished(boolean published) {
        this.published = published;
    }

    public Revision(String revisionName, String revisionProviderId, Date creationTimeStamp, boolean acceptance, boolean published, boolean deleted) {
        this.revisionName = revisionName;
        this.revisionProviderId = revisionProviderId;
        this.creationTimeStamp = creationTimeStamp;
        this.published = published;
        this.deleted = deleted;
        this.acceptance = acceptance;


    }


    public Revision(String revisionName, String providerId) {
        this(revisionName, providerId, new Date(), false, false, false);
    }


    public Revision(final Revision revision) {
        this(revision.getRevisionName(), revision.getRevisionProviderId(), revision.getCreationTimeStamp(), revision.isAcceptance(), revision.isPublished(), revision.isDeleted());
    }


    public String getRevisionName() {
        return revisionName;
    }

    public void setRevisionName(String revisionName) {
        this.revisionName = revisionName;
    }

    public Revision() {
    }

    public String getRevisionProviderId() {
        return revisionProviderId;
    }

    public void setRevisionProviderId(String revisionProviderId) {
        this.revisionProviderId = revisionProviderId;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Revision)) {
            return false;
        }
        Revision revision = (Revision) o;
        if (!revisionName.equals(revision.getRevisionName())) {
            return false;
        }
        if (!revisionProviderId.equals(revision.getRevisionProviderId())) {
            return false;
        }
        if (acceptance != revision.acceptance) {
            return false;
        }
        if (published != revision.published) {
            return false;
        }
        if (deleted != revision.deleted) {
            return false;
        }
        if (creationTimeStamp == null)
            if (revision.getCreationTimeStamp() != null) {
                return false;
            }
        return true;
    }


    @Override
    public int hashCode() {
        int result = 31;
        result = 31 * result + (revisionName != null ? revisionName.hashCode() : 0);
        result = 31 * result + (revisionProviderId != null ? revisionProviderId.hashCode() : 0);
        result = 31 * result + (creationTimeStamp != null ? creationTimeStamp.hashCode() : 0);
        result = 31 * result + (acceptance ? 1 : 0);
        result = 31 * result + (published ? 1 : 0);
        result = 31 * result + (deleted ? 1 : 0);
        return result;
    }


    @Override
    public String toString() {
        return "Revision [revisionNme=" + revisionName + ", revisionProvider=" + revisionProviderId
                + ", creationTimeStamp=" + creationTimeStamp + ", acceptance="
                + acceptance + ", published=" + published + ", deleted=" + deleted + "]";
    }


}
