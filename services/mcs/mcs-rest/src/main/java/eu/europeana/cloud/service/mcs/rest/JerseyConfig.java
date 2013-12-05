package eu.europeana.cloud.service.mcs.rest;

import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spring.scope.RequestContextFilter;

import eu.europeana.cloud.service.mcs.rest.exceptionmappers.*;

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
        register(CannotModifyPersistentRepresentationExceptionMapper.class);
        register(DataSetAlreadyExistsExceptionMapper.class);
        register(DataSetNotExistsExceptionMapper.class);
        register(FileAlreadyExistsExceptionMapper.class);
        register(FileNotExistsExceptionMapper.class);
        register(ProviderHasDataSetsExceptionMapper.class);
        register(ProviderHasRecordsExceptionMapper.class);
        register(ProviderNotExistsExceptionMapper.class);
        register(RecordNotExistsExceptionMapper.class);
        register(RepresentationNotExistsExceptionMapper.class);
        register(VersionNotExistsExceptionMapper.class);
        register(RepresentationAlreadyInSetExceptionMapper.class);
        register(WrongContentRangeExceptionMapper.class);

        // resources
        register(RecordsResource.class);
        register(RepresentationResource.class);
        register(RepresentationsResource.class);
        register(RepresentationVersionResource.class);
        register(RepresentationVersionsResource.class);
        register(RepresentationSearchResource.class);
        register(FilesResource.class);
        register(FileResource.class);
        register(DataProviderResource.class);
        register(DataProvidersResource.class);
        register(DataSetResource.class);
        register(DataSetsResource.class);
        register(DataSetAssignmentsResource.class);
    }
}
