package eu.europeana.cloud.service.mcs.persistent.swift;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.NoSuchElementException;

/**
 * Manage connection for Openstack Swift using jClouds library.
 */
@Component
public class SimpleSwiftConnectionProvider implements SwiftConnectionProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSwiftConnectionProvider.class);
    private BlobStoreContext context;
    private final String container;
    private BlobStore blobStore;

    private final String provider;
    private final String endpoint;
    private final String user;
    private final String password;

    /**
     * Class constructor. Establish connection to Openstack Swift endpoint using provided configuration.
     *
     * @param provider provider name. Pass "transient" if you want to use in-memory implementation for tests,
     *                and "swift" for accessing live Openstack Swift.
     * @param container name of the Swift container (namespace)
     * @param endpoint swift endpoint URL
     * @param user user identity
     * @param password user password
     */
    public SimpleSwiftConnectionProvider(String provider, String container, String endpoint, String user, String password) {
        this.container = container;
        this.provider = provider;
        this.endpoint = endpoint;
        this.user = user;
        this.password = password;
        openConnections();
    }

    private void openConnections() throws NoSuchElementException {
        context = ContextBuilder
                .newBuilder(provider)
                .endpoint(endpoint)
                .credentials(user, password)
                .buildView(BlobStoreContext.class);

        blobStore = context.getBlobStore();
        if (!blobStore.containerExists(container)) {
            blobStore.createContainerInLocation(null, container);
        }
        LOGGER.info("Connected to swift.");
    }

    @Override
    @PreDestroy
    public void closeConnections() {
        LOGGER.info("Shutting down swift connection");
        context.close();
    }

    @Override
    public String getContainer() {
        return container;
    }


    @Override
    public BlobStore getBlobStore() {
        return blobStore;
    }

}
