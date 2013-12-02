package eu.europeana.cloud.service.mcs;

import java.util.Date;
import java.util.Objects;

/**
 * Parameter Object grouping parameters used to search representations. Instances of this class are immutable and must
 * be created with {@link Builder}.
 */
public class RepresentationSearchParams {

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
	 * Start of representation version creation date range.
	 */
	private final Date fromDate;

	/**
	 * End of representation version creation date range.
	 */
	private final Date toDate;


	private RepresentationSearchParams(String schema, String dataProvider, Boolean persistent, String dataSetId, Date fromDate, Date toDate) {
		this.schema = schema;
		this.dataProvider = dataProvider;
		this.persistent = persistent;
		this.dataSetId = dataSetId;
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
		int hash = 7;
		hash = 89 * hash + Objects.hashCode(this.schema);
		hash = 89 * hash + Objects.hashCode(this.dataProvider);
		hash = 89 * hash + Objects.hashCode(this.persistent);
		hash = 89 * hash + Objects.hashCode(this.dataSetId);
		hash = 89 * hash + Objects.hashCode(this.fromDate);
		hash = 89 * hash + Objects.hashCode(this.toDate);
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
		if (!Objects.equals(this.fromDate, other.fromDate)) {
			return false;
		}
		if (!Objects.equals(this.toDate, other.toDate)) {
			return false;
		}
		return true;
	}

	/**
	 * Builder class - to create immutable instances of {@link RepresentationSearchParams}.
	 */
	public static class Builder {

		private String schema;

		private String dataProvider;

		private Boolean persistent;

		private String dataSetId;

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


		public Builder setDataSetId(String dataSetId) {
			this.dataSetId = dataSetId;
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
			return new RepresentationSearchParams(schema, dataProvider, persistent, dataSetId, fromDate, toDate);
		}

	}
}
