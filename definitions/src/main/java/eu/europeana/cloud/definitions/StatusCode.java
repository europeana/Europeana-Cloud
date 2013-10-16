package eu.europeana.cloud.definitions;

import javax.ws.rs.core.Response.Status;

/**
 * Status Messages returned by all methods
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public enum StatusCode {

	/**
	 * OK message - HTTP Code: 200
	 */
	OK {
		@Override
		public String getDescription(String... args) {

			return "Operation finished successfully";
		}

		@Override
		public Status getHttpCode() {
			return Status.OK;
		}

	},
	/**
	 * Unspecified Error - HTTP Code: 500
	 */
	GENERICERROR {
		@Override
		public String getDescription(String... args) {
			return String.format("Unspecified Error occured with message %s",
					args[0]);
		}

		@Override
		public Status getHttpCode() {
			return Status.INTERNAL_SERVER_ERROR;
		}
	},
	/**
	 * Database Connection Error - HTTP Code: 500
	 */
	DATABASECONNECTIONERROR {
		@Override
		public String getDescription(String... args) {
			return String.format(
					"The connection to the DB %s:%s/%s failed with error %s",
					args[0], args[1], args[2], args[3]);
		}

		@Override
		public Status getHttpCode() {
			return Status.INTERNAL_SERVER_ERROR;
		}
	},
	/**
	 * Record exists already in the database - HTTP code: 409
	 */
	RECORDEXISTS {
		@Override
		public String getDescription(String... args) {
			return String
					.format("The identifier %s for provider id %s and record id %s already exists in the database",
							args[0], args[1], args[2]);
		}

		@Override
		public Status getHttpCode() {
			return Status.CONFLICT;
		}
	},
	/**
	 * The record does not exist in the database - HTTP code: 404
	 */
	RECORDDOESNOTEXIST {
		@Override
		public String getDescription(String... args) {
			return String
					.format("A global identifier for provider id %s and record id %s does not exist",
							args[0], args[1]);
		}

		@Override
		public Status getHttpCode() {
			return Status.NOT_FOUND;
		}
	},
	/**
	 * The supplied unique identifier does not exist - HTTP code: 404
	 */
	GLOBALIDDOESNOTEXIST {
		@Override
		public String getDescription(String... args) {
			return String
					.format("The supplied global identifier %s does not exist",
							args[0]);
		}

		@Override
		public Status getHttpCode() {
			return Status.NOT_FOUND;
		}
	},
	/**
	 * The provider id does not exist - HTTP code: 404
	 */
	PROVIDERDOESNOTEXIST {
		@Override
		public String getDescription(String... args) {
			return String.format(
					"The supplied provider identifier %s does not exist",
					args[0]);
		}

		@Override
		public Status getHttpCode() {
			return Status.NOT_FOUND;
		}
	},
	/**
	 * The record id does not exist - HTTP code: 404
	 */
	RECORDIDDOESNOTEXIST {
		@Override
		public String getDescription(String... args) {
			return String
					.format("The supplied record identifier %s does not exist",
							args[0]);
		}

		@Override
		public Status getHttpCode() {
			return Status.NOT_FOUND;
		}
	},
	/**
	 * The requested record set for the provider id is empty - HTTP code: 404
	 */
	RECORDSETEMPTY {
		@Override
		public String getDescription(String... args) {
			return String
					.format("The supplied provider %s does not have any records associated with it",
							args[0]);
		}

		@Override
		public Status getHttpCode() {
			return Status.NOT_FOUND;
		}
	},
	/**
	 * The combination of provider id/ record id has already been mapped to
	 * another unique identifier - HTTP code: 409
	 */
	IDHASBEENMAPPED {

		@Override
		public String getDescription(String... args) {
			return String
					.format("The supplied %s id for provider %s has already been assigned to the global identifier %s",
							args[0], args[1], args[2]);
		}

		@Override
		public Status getHttpCode() {
			return Status.CONFLICT;
		}

	};

	/**
	 * Return a predefined human readable error message
	 * @param args
	 * @return
	 */
	public abstract String getDescription(String... args);

	/**
	 * Return the HTTP code
	 * @return
	 */
	public abstract Status getHttpCode();
}