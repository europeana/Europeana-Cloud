package eu.europeana.cloud.service.mcs.persistent.swift;

import javax.annotation.PreDestroy;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Manage connection for Openstack Swift using jClouds library.
 */
@Component
public class SwiftConnectionProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(SwiftConnectionProvider.class);
    private final BlobStoreContext context;
    private final String container;
    private final BlobStore blobStore;


    /**
     * Class constructor. Establish connection to Openstack Swift endpoint using provided configuration.
     * 
     * @param provider
     *            provider name. Pass "transient" if you want to use in-memory implementation for tests, and "swift" for
     *            accessing live Openstack Swift.
     * @param container
     *            name of the Swift container (namespace)
     * @param endpoint
     *            Swift endpoint URL
     * @param user
     *            user identity
     * @param password
     *            user password
     */
    public SwiftConnectionProvider(String provider, String container, String endpoint, String user, String password) {
        this.container = container;
        context = ContextBuilder.newBuilder(provider).endpoint(endpoint).credentials(user, password)
                .buildView(BlobStoreContext.class);
        blobStore = context.getBlobStore();
        if (!blobStore.containerExists(container)) {
            blobStore.createContainerInLocation(null, container);
        }
        LOGGER.info("Connected to swift");
    }


    /**
     * Close connection on container destroy.
     */
    @PreDestroy
    private void closeConnections() {
        LOGGER.info("Shutting down swift connection");
        context.close();
    }


    public BlobStoreContext getContext() {
        return context;
    }


    public String getContainer() {
        return container;
    }


    public BlobStore getBlobStore() {
        return blobStore;
    }

}
