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


    /**
     * Returns object hashcode.
     * 
     * @return hashCode
     */
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


    /**
     * Check if this object is equal to provided one.
     * 
     * @param obj
     *            object to compare
     * @return true if objects are equal, false otherwise
     */
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


    /**
     * Return parameters as human readable string.
     * 
     * @return string representing object in human readable form.
     */
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


        /**
         * Constructor.
         */
        public Builder() {
        }


        /**
         * Sets schema.
         * 
         * @param schema
         * @return
         */
        public Builder setSchema(String schema) {
            this.schema = schema;
            return this;
        }


        /**
         * Sets data provider.
         * 
         * @param dataProvider
         * @return
         */
        public Builder setDataProvider(String dataProvider) {
            this.dataProvider = dataProvider;
            return this;
        }


        /**
         * Sets if representation is persistent.
         * 
         * @param persistent
         * @return
         */
        public Builder setPersistent(Boolean persistent) {
            this.persistent = persistent;
            return this;
        }


        /**
         * Sets record id.
         * 
         * @param recordId
         * @return
         */
        public Builder setRecordId(String recordId) {
            this.recordId = recordId;
            return this;
        }


        /**
         * Sets data set id.
         * 
         * @param dataSetId
         * @return
         */
        public Builder setDataSetId(String dataSetId) {
            this.dataSetId = dataSetId;
            return this;
        }


        /**
         * Sets provider id.
         * 
         * @param dataSetProviderId
         * @return
         */
        public Builder setDataSetProviderId(String dataSetProviderId) {
            this.dataSetProviderId = dataSetProviderId;
            return this;
        }


        /**
         * Sets fromDate.
         * 
         * @param fromDate
         * @return
         */
        public Builder setFromDate(Date fromDate) {
            this.fromDate = fromDate;
            return this;
        }


        /**
         * Sets toDate.
         * 
         * @param toDate
         * @return
         */
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
