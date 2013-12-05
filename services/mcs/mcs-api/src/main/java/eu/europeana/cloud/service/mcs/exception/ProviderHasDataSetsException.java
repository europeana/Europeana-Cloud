package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown if there is attempt to delete a data provider which has some data sets. In order to delete data provider, all
 * his data sets must be implicitly removed first.
 */
public class ProviderHasDataSetsException extends RuntimeException {

    public ProviderHasDataSetsException() {
    }


    public ProviderHasDataSetsException(String message) {
        super(message);
    }
}
