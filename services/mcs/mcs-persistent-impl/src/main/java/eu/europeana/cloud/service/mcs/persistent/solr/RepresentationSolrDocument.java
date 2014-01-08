package eu.europeana.cloud.service.mcs.persistent.solr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Objects;

import org.apache.solr.client.solrj.beans.Field;

import eu.europeana.cloud.common.model.Representation;

/**
 * Bean representing Representation + its data set assignments for SOLR indexing purposes (to be used only in
 * communication with solr index). All fields have the same meaning as in {@link Representation} class and
 * {@link #dataSets} contain a collection of data sets which this representation is currently assigned to. Because data
 * set has unique identivier only with provider id, those two identifiers must be somehow combined into one string
 * element in this data sets collection.
 */
public class RepresentationSolrDocument {

    @Field(SolrFields.CLOUD_ID)
    private String cloudId;

    @Field(SolrFields.VERSION)
    private String version;

    @Field(SolrFields.SCHEMA)
    private String schema;

    @Field(SolrFields.PROVIDER_ID)
    private String providerId;

    @Field(SolrFields.CREATION_DATE)
    private Date creationDate;

    @Field(SolrFields.PERSISTENT)
    private boolean persistent;

    @Field(SolrFields.DATA_SETS)
    private Collection<String> dataSets = new ArrayList<>();


    public String getCloudId() {
        return cloudId;
    }


    public void setCloudId(String cloudId) {
        this.cloudId = cloudId;
    }


    public String getVersion() {
        return version;
    }


    public void setVersion(String version) {
        this.version = version;
    }


    public String getSchema() {
        return schema;
    }


    public void setSchema(String schema) {
        this.schema = schema;
    }


    public String getProviderId() {
        return providerId;
    }


    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }


    public Date getCreationDate() {
        return creationDate;
    }


    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }


    public boolean isPersistent() {
        return persistent;
    }


    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }


    public Collection<String> getDataSets() {
        return dataSets;
    }


    public void setDataSets(Collection<String> dataSets) {
        this.dataSets = dataSets;
    }


    public RepresentationSolrDocument() {
    }


    public RepresentationSolrDocument(String cloudId, String version, String schema, String providerId,
            Date creationDate, boolean persistent) {
        this.cloudId = cloudId;
        this.version = version;
        this.schema = schema;
        this.providerId = providerId;
        this.creationDate = creationDate;
        this.persistent = persistent;
    }


    public RepresentationSolrDocument(String cloudId, String version, String schema, String providerId,
            Date creationDate, boolean persistent, Collection<String> dataSets) {
        this(cloudId, version, schema, providerId, creationDate, persistent);
        this.dataSets = dataSets;
    }


    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.cloudId);
        hash = 97 * hash + Objects.hashCode(this.version);
        hash = 97 * hash + Objects.hashCode(this.schema);
        hash = 97 * hash + Objects.hashCode(this.providerId);
        hash = 97 * hash + Objects.hashCode(this.creationDate);
        hash = 97 * hash + (this.persistent ? 1 : 0);
        hash = 97 * hash + Objects.hashCode(this.dataSets);
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
        final RepresentationSolrDocument other = (RepresentationSolrDocument) obj;
        if (!Objects.equals(this.cloudId, other.cloudId)) {
            return false;
        }
        if (!Objects.equals(this.version, other.version)) {
            return false;
        }
        if (!Objects.equals(this.schema, other.schema)) {
            return false;
        }
        if (!Objects.equals(this.providerId, other.providerId)) {
            return false;
        }
        if (!Objects.equals(this.creationDate, other.creationDate)) {
            return false;
        }
        if (this.persistent != other.persistent) {
            return false;
        }
        if (!Objects.equals(this.dataSets, other.dataSets)) {
            return false;
        }
        return true;
    }


    @Override
    public String toString() {
        return "RepresentationSolrDocument{" + "cloudId=" + cloudId + ", version=" + version + ", schema=" + schema
                + ", providerId=" + providerId + ", creationDate=" + creationDate + ", persistent=" + persistent
                + ", dataSets=" + dataSets + '}';
    }

}
