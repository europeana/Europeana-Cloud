package eu.europeana.cloud.service.mcs.persistent;

import java.util.Date;
import java.util.Objects;

/**
 *
 * @author sielski
 */
public class RepresentationSearchParams {

	private final String schema;

	private final String dataProvider;

	private final Boolean persistent;

	private final String dataSetId;

	private final Date fromDate;

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


		public RepresentationSearchParams build() {
			return new RepresentationSearchParams(schema, dataProvider, persistent, dataSetId, fromDate, toDate);
		}

	}
}
