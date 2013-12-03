package eu.europeana.cloud.service.mcs;

import java.util.Date;
import java.util.Objects;

/**
 * Parameter Object grouping parameters used to search representations. Instances of this class are immutable and must
 * be created with {@link Builder}.
 */
public class RepresentationSearchParams {

	/**
	 * Record id.
	 */
	private final String recordId;

	/**
	 * Representation schema.
	 */
	private final String schema;

	/**
	 * Representation version provider id.
	 */
	private final String dataProvider;

	/**
	 * Representation version state.
	 */
	private final Boolean persistent;

	/**
	 * Identifier of data set.
	 */
	private final String dataSetId;

	/**
	 * Identifier of owner (provider) of data set.
	 */
	private final String dataSetProviderId;

	/**
	 * Start of representation version creation date range.
	 */
	private final Date fromDate;

	/**
	 * End of representation version creation date range.
	 */
	private final Date toDate;


	private RepresentationSearchParams(String recordId, String schema, String dataProvider, Boolean persistent, String dataSetId,
			String dataSetProviderId, Date fromDate, Date toDate) {
		this.recordId = recordId;
		this.schema = schema;
		this.dataProvider = dataProvider;
		this.persistent = persistent;
		this.dataSetId = dataSetId;
		this.dataSetProviderId = dataSetProviderId;
		this.fromDate = fromDate;
		this.toDate = toDate;
	}


	public String getSchema() {
		return schema;
	}


	public String getDataProvider() {
		return dataProvider;
	}


	public Boolean isPersistent() {
		return persistent;
	}


	public String getDataSetId() {
		return dataSetId;
	}


	public Date getFromDate() {
		return fromDate;
	}


	public Date getToDate() {
		return toDate;
	}


	public String getRecordId() {
		return recordId;
	}




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
		return "RepresentationSearchParams{" + "recordId=" + recordId + ", schema=" + schema + ", dataProvider=" + dataProvider + ", persistent=" + persistent + ", dataSetId=" + dataSetId + ", dataSetProviderId=" + dataSetProviderId + ", fromDate=" + fromDate + ", toDate=" + toDate + '}';
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
			return new RepresentationSearchParams(recordId, schema, dataProvider, persistent, dataSetId, dataSetProviderId, fromDate, toDate);
		}

	}
}
