package eu.europeana.cloud.service.mcs.persistent.swift;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;

/**
 * Manage connection for Openstack Swift endpoints using jClouds library.
 */
public interface SwiftConnectionProvider {

    /**
     * @return {@link BlobStore}
     */
    BlobStore getBlobStore();

    /**
     * @return name of container
     */
    String getContainer();

    /**
     * Close connection on container destroy.
     */
    public void closeConnections();

}
