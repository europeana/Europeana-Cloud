package eu.europeana.cloud.service.mcs.persistent.swift;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.aspectj.lang.annotation.Before;
import org.jclouds.blobstore.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Manage connection for Openstack Swift to multiple endpoints using jClouds
 * library.
 */
@Component
public class DynamicSwiftConnectionProvider implements SwiftConnectionProvider {

    private static final Logger LOGGER = LoggerFactory
	    .getLogger(DynamicSwiftConnectionProvider.class);
    private final List<SwiftConnectionProvider> proxys;
    private List<BlobStore> blobStores;

    @Autowired(required = true)
    private DynamicBlobStore dynamicBlobStore;

    private final String container;

    /**
     * Class constructor. Establish connection to multi Openstack Swift
     * endpoints using provided configuration.
     * 
     * @param provider
     *            provider name. Pass "transient" if you want to use in-memory
     *            implementation for tests, and "swift" for accessing live
     *            Openstack Swift.
     * @param container
     *            name of the Swift container (namespace)
     * @param endpointsList
     *            list of Swift endpoint URLs
     * @param user
     *            user identity
     * @param password
     *            user password
     */
    public DynamicSwiftConnectionProvider(String provider,
            String container, String endpointsList, String user, String password) {

        String[] endpoints = endpointsList.split(",");
        proxys = new ArrayList<>();
        blobStores = new ArrayList<>();
        this.container = container;
        for (String endpoint : endpoints) {
            LOGGER.info("Connecing to swift" + endpoint);
            SwiftConnectionProvider connectionProvider = new SimpleSwiftConnectionProvider(
                    provider, container, endpoint, user, password);
            blobStores.add(connectionProvider.getBlobStore());
            proxys.add(connectionProvider);
        }
    }

    /**
     * Initialize {@link DynamicBlobStore}.
     */
    @PostConstruct
    public void setBlobStores() {
	dynamicBlobStore.setBlobStores(blobStores);
    }

    @PreDestroy
    public void closeConnections() {
	for (SwiftConnectionProvider proxy : proxys) {
	    proxy.closeConnections();
	}

    }

    /**
     * {@inheritDoc }
     */
    @Override
    public String getContainer() {
	return container;
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public BlobStore getBlobStore() {
	return dynamicBlobStore;
    }

}
