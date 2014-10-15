package eu.europeana.cloud.service.mcs.persistent.swift;

import java.util.List;
import org.jclouds.blobstore.BlobStore;

public interface DBlobStore extends BlobStore {

    /**
     * Set list of {@link BlobStore}.
     * 
     * @param blobStores
     */
    public void setBlobStores(List<BlobStore> blobStores);

    /**
     * Return number of blobStores.
     * 
     * @return
     */
    public int getInstanceNumber();

    /**
     * Switch cycle between {@link BlobStore}.
     */
    public void switchInstance();
}
