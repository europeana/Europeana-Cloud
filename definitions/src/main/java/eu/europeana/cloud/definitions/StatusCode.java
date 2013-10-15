package eu.europeana.cloud.definitions;

import javax.ws.rs.core.Response.Status;

public enum StatusCode {

	// Operation Successful
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
	// Unspecified Error
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

	public abstract String getDescription(String... args);

	public abstract Status getHttpCode();
}