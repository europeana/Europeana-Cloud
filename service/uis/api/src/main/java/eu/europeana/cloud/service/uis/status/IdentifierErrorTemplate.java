package eu.europeana.cloud.service.uis.status;

import javax.ws.rs.core.Response.Status;

import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Status Messages returned by all methods
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 22, 2013
 */
public enum IdentifierErrorTemplate {

	/**
	 * Unspecified Error - HTTP Code: 500
	 */
	GENERIC_ERROR {
		@Override
		public ErrorInfo getErrorInfo(String... args) {
			return new ErrorInfo("GENERIC_ERROR", String.format("Unspecified Error occured with message %s", args[0]));
		}

		@Override
		public Status getHttpCode() {
			return Status.INTERNAL_SERVER_ERROR;
		}
		
	},

	/**
	 * Database Connection Error - HTTP Code: 500
	 */
	DATABASE_CONNECTION_ERROR {
		@Override
		public ErrorInfo getErrorInfo(String... args) {
			return new ErrorInfo("DATABASE_CONNECTION_ERROR", String.format(
					"The connection to the DB %s/%s failed with error %s", args[0], args[1], args[2]));
		}

		@Override
		public Status getHttpCode() {
			return Status.INTERNAL_SERVER_ERROR;
		}
	},

	/**
	 * Record exists already in the database - HTTP code: 409
	 */
	RECORD_EXISTS {
		@Override
		public ErrorInfo getErrorInfo(String... args) {
			return new ErrorInfo("RECORD_EXISTS", String.format(
					"An identifier for provider id %s and record id %s already exists in the database", args[0],
					args[1]));
		}

		@Override
		public Status getHttpCode() {
			return Status.CONFLICT;
		}
	},

	/**
	 * The record does not exist in the database - HTTP code: 404
	 */
	RECORD_DOES_NOT_EXIST {
		@Override
		public ErrorInfo getErrorInfo(String... args) {
			return new ErrorInfo("RECORD_DOES_NOT_EXIST", String.format(
					"A global identifier for provider id %s and record id %s does not exist", args[0], args[1]));
		}

		@Override
		public Status getHttpCode() {
			return Status.NOT_FOUND;
		}
	},

	/**
	 * The supplied unique identifier does not exist - HTTP code: 404
	 */
	CLOUDID_DOES_NOT_EXIST {
		@Override
		public ErrorInfo getErrorInfo(String... args) {
			return new ErrorInfo("CLOUDID_DOES_NOT_EXIST", String.format(
					"The supplied cloud identifier %s does not exist", args[0]));
		}

		@Override
		public Status getHttpCode() {
			return Status.NOT_FOUND;
		}
	},

	/**
	 * The provider id does not exist - HTTP code: 404
	 */
	PROVIDER_DOES_NOT_EXIST {
		@Override
		public ErrorInfo getErrorInfo(String... args) {
			return new ErrorInfo("PROVIDER_DOES_NOT_EXIST", String.format(
					"The supplied provider identifier %s does not exist", args[0]));
		}

		@Override
		public Status getHttpCode() {
			return Status.NOT_FOUND;
		}
	},
	/**
	 * The provider already exists exception
	 */
	PROVIDER_ALREADY_EXISTS {

		@Override
		public ErrorInfo getErrorInfo(String... args) {
			return new ErrorInfo("PROVIDER_ALREADY_EXISTS",String.format("The provider with identifier %s already exists", args[0]));
		}

		@Override
		public Status getHttpCode() {
			return Status.CONFLICT;
		}
		
	},

	/**
	 * The record id does not exist - HTTP code: 404
	 */
	RECORDID_DOES_NOT_EXIST {
		@Override
		public ErrorInfo getErrorInfo(String... args) {
			return new ErrorInfo("RECORDID_DOES_NOT_EXIST", String.format(
					"The supplied record identifier %s does not exist", args[0]));
		}

		@Override
		public Status getHttpCode() {
			return Status.NOT_FOUND;
		}
	},

	/**
	 * The requested record set for the provider id is empty - HTTP code: 404
	 */
	RECORDSET_EMPTY {
		@Override
		public ErrorInfo getErrorInfo(String... args) {
			return new ErrorInfo("RECORDSET_EMPTY", String.format(
					"The supplied provider %s does not have any records associated with it", args[0]));
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
	ID_HAS_BEEN_MAPPED {
		@Override
		public ErrorInfo getErrorInfo(String... args) {
			return new ErrorInfo("ID_HAS_BEEN_MAPPED", String.format(
					"The supplied %s id for provider %s has already been assigned to the cloud identifier %s", args[0],
					args[1], args[2]));
		}

		@Override
		public Status getHttpCode() {
			return Status.CONFLICT;
		}
	};

	/**
	 * Generate the error message for each case
	 * @param args
	 * @return The generated error message
	 */
	public abstract ErrorInfo getErrorInfo(String... args);

	/**
	 * Return the a ErrorInfo with the message or reply
	 * 
	 * @param args
	 *            string arguments to be filled into the message
	 * @return The error message and description
	 */

	/**
	 * Return the according HTTP Code
	 * 
	 * @return The relevant HTTP Code
	 */
	public abstract Status getHttpCode();
	
}