package eu.europeana.cloud.service.mcs.rest;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spring.scope.RequestContextFilter;

public class JerseyConfig extends ResourceConfig {

    /**
     * Register JAX-RS application components.
     */
    public JerseyConfig() {
        register(RequestContextFilter.class);
        register(CustomExceptionMapper.class);
        register(MultiPartFeature.class);
        register(RecordsResource.class);
        register(RepresentationResource.class);
        register(RepresentationsResource.class);
        register(RepresentationVersionResource.class);
        register(RepresentationVersionsResource.class);
        register(FilesResource.class);
        register(FileResource.class);
        register(DataProviderResource.class);
        register(DataProvidersResource.class);
    }
}
