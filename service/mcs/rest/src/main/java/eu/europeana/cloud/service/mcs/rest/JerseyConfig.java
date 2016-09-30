package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.service.commons.logging.LoggingFilter;
import eu.europeana.cloud.service.mcs.exception.RevisionNotExistsException;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.*;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spring.scope.RequestContextFilter;

/**
 * Jersey Configuration for Exception Mappers and Resources
 */
public class JerseyConfig extends ResourceConfig {

    /**
     * Register JAX-RS application components.
     */
    public JerseyConfig() {
        super();
        //features
        register(MultiPartFeature.class);

        // filters
        register(RequestContextFilter.class);
        register(LoggingFilter.class);

        // exception mappers
        register(CannotPersistEmptyRepresentationExceptionMapper.class);
        register(CannotModifyPersistentRepresentationExceptionMapper.class);
        register(CannotPersistEmptyRepresentationExceptionMapper.class);
        register(DataSetAlreadyExistsExceptionMapper.class);
        register(DataSetNotExistsExceptionMapper.class);
        register(FileAlreadyExistsExceptionMapper.class);
        register(FileNotExistsExceptionMapper.class);
        register(RecordNotExistsExceptionMapper.class);
        register(RepresentationNotExistsExceptionMapper.class);
        register(VersionNotExistsExceptionMapper.class);
        register(WrongContentRangeExceptionMapper.class);
        register(ProviderNotExistsExceptionMapper.class);
        register(WebApplicationExceptionMapper.class);
        register(RuntimeExceptionMapper.class);
        registerClasses(RevisionIsNotValidExceptionMapper.class);
        register(RevisionNotExistsExceptionMapper.class);

        // resources
        register(RecordsResource.class);
        register(RepresentationResource.class);
        register(RepresentationsResource.class);
        register(RepresentationVersionResource.class);
        register(RepresentationVersionsResource.class);
        register(RepresentationAuthorizationResource.class);
        register(FilesResource.class);
        register(FileResource.class);
        register(FileUploadResource.class);
        register(DataSetResource.class);
        register(DataSetsResource.class);
        register(DataSetAssignmentsResource.class);
        register(SimplifiedFileAccessResource.class);
        register(SimplifiedRecordsResource.class);
        register(SimplifiedRepresentationResource.class);
        registerClasses(RevisionResource.class);
        registerClasses(RepresentationRevisionsResource.class);
    }
}
