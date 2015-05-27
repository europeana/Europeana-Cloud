package eu.europeana.cloud.service.mcs.persistent.swift;

import java.util.List;
import org.jclouds.blobstore.BlobStore;

/**
 * Interface to switch betewene {@link BlobStore} instances.
 */
public interface DBlobStore extends BlobStore {

    /**
     * List of {@link BlobStore}.
     * 
     * @param blobStores
     *            list of {@link BlobStore}.
     */
    void setBlobStores(List<BlobStore> blobStores);


    /**
     * Return number of blobStores.
     * 
     * @return number of blobStores
     */
    int getInstanceNumber();


    /**
     * Switch cycle between {@link BlobStore}.
     */
    void switchInstance();


    /**
     * Return {@link DynamicBlobStore} without current instance.
     * 
     * @return {@link DynamicBlobStore} without current instance.
     */
    DynamicBlobStore getDynamicBlobStoreWithoutActiveInstance();
}
