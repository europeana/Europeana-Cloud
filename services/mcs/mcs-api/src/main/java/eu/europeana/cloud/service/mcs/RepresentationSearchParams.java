package eu.europeana.cloud.service.mcs;

import java.util.Date;
import java.util.Objects;

/**
 * Parameter Object grouping parameters used to search representations. Instances of this class are immutable and must
 * be created with {@link Builder}.
 */
public final class RepresentationSearchParams {

    private final String recordId;

    private final String schema;

    private final String dataProvider;

    private final Boolean persistent;

    private final String dataSetId;

    private final String dataSetProviderId;

    private final Date fromDate;

    private final Date toDate;


    private RepresentationSearchParams(String recordId, String schema, String dataProvider, Boolean persistent,
            String dataSetId, String dataSetProviderId, Date fromDate, Date toDate) {
        this.recordId = recordId;
        this.schema = schema;
        this.dataProvider = dataProvider;
        this.persistent = persistent;
        this.dataSetId = dataSetId;
        this.dataSetProviderId = dataSetProviderId;
        this.fromDate = fromDate;
        this.toDate = toDate;
    }


    /**
     * Returns representation schema.
     */
    public String getSchema() {
        return schema;
    }


    /**
     * Returns representation version provider id.
     */
    public String getDataProvider() {
        return dataProvider;
    }


    /**
     * Returns {@code true} if representation is persistent.
     */
    public Boolean isPersistent() {
        return persistent;
    }


    /**
     * Returns identifier of data set.
     */
    public String getDataSetId() {
        return dataSetId;
    }


    /**
     * Returns start of representation version creation date range.
     */
    public Date getFromDate() {
        return fromDate;
    }


    /**
     * Returns end of representation version creation date range.
     */
    public Date getToDate() {
        return toDate;
    }


    /**
     * Returns record identifier.
     */
    public String getRecordId() {
        return recordId;
    }


    /**
     * Returns identifier of owner (provider) of data set.
     */
    public String getDataSetProviderId() {
        return dataSetProviderId;
    }


    /**
     * Shortcut to {@link Builder#Builder()}. Returns new instance of {@link Builder}.
     * 
     * @return
     */
    public static Builder builder() {
        return new Builder();
    }


    @Override
    public int hashCode() {
        int hash = 3;
        hash = 11 * hash + Objects.hashCode(this.recordId);
        hash = 11 * hash + Objects.hashCode(this.schema);
        hash = 11 * hash + Objects.hashCode(this.dataProvider);
        hash = 11 * hash + Objects.hashCode(this.persistent);
        hash = 11 * hash + Objects.hashCode(this.dataSetId);
        hash = 11 * hash + Objects.hashCode(this.dataSetProviderId);
        hash = 11 * hash + Objects.hashCode(this.fromDate);
        hash = 11 * hash + Objects.hashCode(this.toDate);
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
        final RepresentationSearchParams other = (RepresentationSearchParams) obj;
        if (!Objects.equals(this.recordId, other.recordId)) {
            return false;
        }
        if (!Objects.equals(this.schema, other.schema)) {
            return false;
        }
        if (!Objects.equals(this.dataProvider, other.dataProvider)) {
            return false;
        }
        if (!Objects.equals(this.persistent, other.persistent)) {
            return false;
        }
        if (!Objects.equals(this.dataSetId, other.dataSetId)) {
            return false;
        }
        if (!Objects.equals(this.dataSetProviderId, other.dataSetProviderId)) {
            return false;
        }
        if (!Objects.equals(this.fromDate, other.fromDate)) {
            return false;
        }
        if (!Objects.equals(this.toDate, other.toDate)) {
            return false;
        }
        return true;
    }


    @Override
    public String toString() {
        return "RepresentationSearchParams{" + "recordId=" + recordId + ", schema=" + schema + ", dataProvider="
                + dataProvider + ", persistent=" + persistent + ", dataSetId=" + dataSetId + ", dataSetProviderId="
                + dataSetProviderId + ", fromDate=" + fromDate + ", toDate=" + toDate + '}';
    }


    /**
     * Builder class - to create immutable instances of {@link RepresentationSearchParams}.
     */
    public static class Builder {

        private String recordId;

        private String schema;

        private String dataProvider;

        private Boolean persistent;

        private String dataSetId;

        private String dataSetProviderId;

        private Date fromDate;

        private Date toDate;


        public Builder() {
        }


        public Builder setSchema(String schema) {
            this.schema = schema;
            return this;
        }


        public Builder setDataProvider(String dataProvider) {
            this.dataProvider = dataProvider;
            return this;
        }


        public Builder setPersistent(Boolean persistent) {
            this.persistent = persistent;
            return this;
        }


        public Builder setRecordId(String recordId) {
            this.recordId = recordId;
            return this;
        }


        public Builder setDataSetId(String dataSetId) {
            this.dataSetId = dataSetId;
            return this;
        }


        public Builder setDataSetProviderId(String dataSetProviderId) {
            this.dataSetProviderId = dataSetProviderId;
            return this;
        }


        public Builder setFromDate(Date fromDate) {
            this.fromDate = fromDate;
            return this;
        }


        public Builder setToDate(Date toDate) {
            this.toDate = toDate;
            return this;
        }


        /**
         * Builds new instance of {@link RepresentationSearchParams} with specified parameters.
         * 
         * @return
         */
        public RepresentationSearchParams build() {
            return new RepresentationSearchParams(recordId, schema, dataProvider, persistent, dataSetId,
                    dataSetProviderId, fromDate, toDate);
        }

    }
}
