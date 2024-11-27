package eu.europeana.cloud.service.mcs.persistent.exception;

import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Custom exception class for handling AWS S3 client errors.
 */
public class CustomAwsClientException extends RuntimeException {
    /**
     * Create a custom exception from S3 client exception.
     *
     * @param exception original s3 exception
     */
    public CustomAwsClientException(S3Exception exception) {
        super("Error occurred when handling S3 connection, Exception message: " + exception.getMessage()
                + ", Error code: " + exception.awsErrorDetails().errorCode()
                + ", Service: " + exception.awsErrorDetails().serviceName()
                + ", Error message: " + exception.awsErrorDetails().errorMessage(), exception);
    }
}