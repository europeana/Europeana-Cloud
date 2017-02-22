package eu.europeana.cloud.service.mcs.persistent.exception;

/**
 * Content DAO could not be found.
 *
 * @author krystian.
 */
public class ContentDaoNotFoundException extends RuntimeException {
    public ContentDaoNotFoundException(String message) {
        super(message);
    }
}
