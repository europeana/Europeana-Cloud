package eu.europeana.cloud.definitions;

public enum StatusCode {

	
	// Operation Successful
	OK {
		@Override
		public String getDescription(String... args) {

			return "Operation finished successfully";
		}

		@Override
		public int getHttpCode() {
			return 200;
		}

	},
	// Unspecified Error
	GENERICERROR {
		@Override
		public String getDescription(String... args) {
			return String.format("Unspecified Error occured with message %s", args[0]);
		}

		@Override
		public int getHttpCode() {
			return 1;
		}
	},
	DATABASECONNECTIONERROR {
		@Override
		public String getDescription(String... args) {
			return String.format("The connection to the DB %s:%s/%s failed with error %s", args[0], args[1], args[2],
					args[3]);
		}

		@Override
		public int getHttpCode() {
			return 500;
		}
	},
	RECORDEXISTS {
		@Override
		public String getDescription(String... args) {
			return String.format(
					"The identifier %s for provider id %s and record id %s already exists in the database", args[0],
					args[1], args[2]);
		}

		@Override
		public int getHttpCode() {
			return 409;
		}
	},
	RECORDDOESNOTEXIST {
		@Override
		public String getDescription(String... args) {
			return String.format("A global identifier for provider id %s and record id %s does not exist", args[0],
					args[1]);
		}

		@Override
		public int getHttpCode() {
			return 404;
		}
	},
	GLOBALIDDOESNOTEXIST {
		@Override
		public String getDescription(String... args) {
			return String.format("The supplied global identifier %s does not exist",args[0]);
		}

		@Override
		public int getHttpCode() {
			return 404;
		}
	},
	PROVIDERDOESNOTEXIST {
		@Override
		public String getDescription(String... args) {
			return String.format("The supplied provider identifier %s does not exist", args[0]);
		}

		@Override
		public int getHttpCode() {
			return 404;
		}
	},
	RECORDIDDOESNOTEXIST {
		@Override
		public String getDescription(String... args) {
			return String.format("The supplied record identifier %s does not exist", args[1]);
		}

		@Override
		public int getHttpCode() {
			return 404;
		}
	},
	RECORDSETEMPTY {
		@Override
		public String getDescription(String... args) {
			return String.format("The supplied provider %s does not have any records associated with it", args[0]);
		}

		@Override
		public int getHttpCode() {
			return 404;
		}
	};

	public abstract String getDescription(String... args);

	public abstract int getHttpCode();
}