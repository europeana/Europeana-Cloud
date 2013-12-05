package eu.europeana.cloud.service.mcs.exception;

/**
 * NOT USED ANYMORE. TO BE REMOVED.
 */
@Deprecated
public class RepresentationAlreadyInSetException extends RuntimeException {

    public RepresentationAlreadyInSetException(String message) {
        super(message);
    }


    public RepresentationAlreadyInSetException(String recordId, String representationId, String dataSetId,
            String providerId) {
        super(String.format("Record %s in schema %s is already assigned to dataset %s (%s)", recordId,
            representationId, dataSetId, providerId));
    }


    public RepresentationAlreadyInSetException() {
    }
}
