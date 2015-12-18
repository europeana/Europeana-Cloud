package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.service.commons.logging.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spring.scope.RequestContextFilter;

import eu.europeana.cloud.service.mcs.rest.exceptionmappers.CannotModifyPersistentRepresentationExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.CannotPersistEmptyRepresentationExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.DataSetAlreadyExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.DataSetNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.FileAlreadyExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.FileNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.ProviderNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RecordNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RepresentationNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RuntimeExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.VersionNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.WebApplicationExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.WrongContentRangeExceptionMapper;

/**
 * Jersey Configuration for Exception Mappers and Resources
 * 
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

        // resources
        register(RecordsResource.class);
        register(RepresentationResource.class);
        register(RepresentationsResource.class);
        register(RepresentationVersionResource.class);
        register(RepresentationVersionsResource.class);
        register(RepresentationAuthorizationResource.class);
        register(FilesResource.class);
        register(FileResource.class);
        register(DataSetResource.class);
        register(DataSetsResource.class);
        register(DataSetAssignmentsResource.class);
        register(SimplifiedFileAccessResource.class);
        register(SimplifiedRecordsResource.class);
        register(SimplifiedRepresentationResource.class);
    }
}
