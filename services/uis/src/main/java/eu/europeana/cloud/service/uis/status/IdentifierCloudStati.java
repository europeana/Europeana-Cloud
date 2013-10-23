package eu.europeana.cloud.service.uis.status;

import javax.ws.rs.core.Response.Status;

import eu.europeana.cloud.common.response.CloudStatus;

/**
 * Status Messages returned by all methods
 * 
 * @author Yorgos.Mamakis@kb.nl
 * @since Oct 22, 2013
 */
public enum IdentifierCloudStati {
    /**
     * OK message - HTTP Code: 200
     */
    OK {
        @Override
        public CloudStatus getCloudStatus(String... args) {
            return new CloudStatus("OK", "Operation finished successfully", Status.OK);
        }
    },

    /**
     * Unspecified Error - HTTP Code: 500
     */
    GENERIC_ERROR {
        @Override
        public CloudStatus getCloudStatus(String... args) {
            return new CloudStatus("GENERIC_ERROR", String.format(
                    "Unspecified Error occured with message %s", args[0]),
                    Status.INTERNAL_SERVER_ERROR);
        }
    },

    /**
     * Database Connection Error - HTTP Code: 500
     */
    DATABASE_CONNECTION_ERROR {
        @Override
        public CloudStatus getCloudStatus(String... args) {
            return new CloudStatus("DATABASE_CONNECTION_ERROR", String.format(
                    "The connection to the DB %s:%s/%s failed with error %s", args[0], args[1],
                    args[2], args[3]), Status.INTERNAL_SERVER_ERROR);
        }
    },

    /**
     * Record exists already in the database - HTTP code: 409
     */
    RECORD_EXISTS {
        @Override
        public CloudStatus getCloudStatus(String... args) {
            return new CloudStatus(
                    "RECORD_EXISTS",
                    String.format(
                            "An identifier for provider id %s and record id %s already exists in the database",
                            args[0], args[1]), Status.CONFLICT);
        }
    },

    /**
     * The record does not exist in the database - HTTP code: 404
     */
    RECORD_DOES_NOT_EXIST {
        @Override
        public CloudStatus getCloudStatus(String... args) {
            return new CloudStatus("RECORD_DOES_NOT_EXIST", String.format(
                    "A global identifier for provider id %s and record id %s does not exist",
                    args[0], args[1]), Status.NOT_FOUND);
        }
    },

    /**
     * The supplied unique identifier does not exist - HTTP code: 404
     */
    GLOBALID_DOES_NOT_EXIST {
        @Override
        public CloudStatus getCloudStatus(String... args) {
            return new CloudStatus("GLOBALID_DOES_NOT_EXIST", String.format(
                    "The supplied global identifier %s does not exist", args[0]), Status.NOT_FOUND);
        }
    },

    /**
     * The provider id does not exist - HTTP code: 404
     */
    PROVIDER_DOES_NOT_EXIST {
        @Override
        public CloudStatus getCloudStatus(String... args) {
            return new CloudStatus("PROVIDER_DOES_NOT_EXIST", String.format(
                    "The supplied provider identifier %s does not exist", args[0]),
                    Status.NOT_FOUND);
        }
    },

    /**
     * The record id does not exist - HTTP code: 404
     */
    RECORDID_DOES_NOT_EXIST {
        @Override
        public CloudStatus getCloudStatus(String... args) {
            return new CloudStatus("RECORDID_DOES_NOT_EXIST", String.format(
                    "The supplied record identifier %s does not exist", args[0]), Status.NOT_FOUND);
        }
    },

    /**
     * The requested record set for the provider id is empty - HTTP code: 404
     */
    RECORDSET_EMPTY {
        @Override
        public CloudStatus getCloudStatus(String... args) {
            return new CloudStatus("RECORDSET_EMPTY", String.format(
                    "The supplied provider %s does not have any records associated with it",
                    args[0]), Status.NOT_FOUND);
        }
    },

    /**
     * The combination of provider id/ record id has already been mapped to another unique
     * identifier - HTTP code: 409
     */
    ID_HAS_BEEN_MAPPED {
        @Override
        public CloudStatus getCloudStatus(String... args) {
            return new CloudStatus(
                    "ID_HAS_BEEN_MAPPED",
                    String.format(
                            "The supplied %s id for provider %s has already been assigned to the global identifier %s",
                            args[0], args[1], args[2]), Status.CONFLICT);
        }
    };

    /**
     * Return the a StatusCode object with the message or reply
     * 
     * @param args
     *            string arguments to be filled into the message
     * @return http code
     */
    public abstract CloudStatus getCloudStatus(String... args);
}