package eu.europeana.cloud.service.mcs.persistent.swift;

import com.google.common.collect.Iterators;
import eu.europeana.cloud.service.mcs.persistent.aspects.RetryBlobStoreExecutor;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.domain.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Manage switching between many jClouds {@link BlobStore}.
 */
public class DynamicBlobStore implements DBlobStore {

    private List<BlobStore> blobStores;
    private BlobStore activeInstance;
    private Iterator<BlobStore> blobIterator;
    private static final Logger LOGGER = LoggerFactory
	    .getLogger(DynamicBlobStore.class);

    /**
     * Set list of {@link BlobStore}.
     * 
     * @param blobStores
     */
    public void setBlobStores(List<BlobStore> blobStores) {
	this.blobStores = blobStores;
	blobIterator = Iterators.cycle(blobStores);
	activeInstance = blobIterator.next();
	// @TODO zookeeper
    }

    /**
     * {@inheritDoc }
     */
    public int getInstanceNumber() {
	return blobStores.size();
    }

    /**
     * {@inheritDoc }
     */
    public void switchInstance() {
	// @TODO zookeeper
	activeInstance = blobIterator.next();
	LOGGER.info("Switch OpenStack Endpoint Instance");
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public BlobStoreContext getContext() {
	return activeInstance.getContext();
    }

    /**
     * {@inheritDoc }
     */
    @RetryOnFailure
    @Override
    public boolean blobExists(String container, String name) {
	return activeInstance.blobExists(container, name);
    }

    /**
     * {@inheritDoc }
     */
    @RetryOnFailure
    @Override
    public BlobBuilder blobBuilder(String name) {
	return activeInstance.blobBuilder(name);
    }

    /**
     * {@inheritDoc }
     */
    @RetryOnFailure
    @Override
    public String putBlob(String container, Blob blob) {
	BlobStore bs = activeInstance;
	return bs.putBlob(container, blob);

    }

    /**
     * {@inheritDoc }
     */
    @RetryOnFailure
    @Override
    public Blob getBlob(String container, String name) {
	return activeInstance.getBlob(container, name);

    }

    /**
     * {@inheritDoc }
     */
    @RetryOnFailure
    @Override
    public Blob getBlob(String container, String name, GetOptions options) {
	return activeInstance.getBlob(container, name, options);
    }

    /**
     * {@inheritDoc }
     */
    @RetryOnFailure
    @Override
    public void removeBlob(String container, String name) {
	activeInstance.removeBlob(container, name);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public Set<? extends Location> listAssignableLocations() {
	return activeInstance.listAssignableLocations();
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public PageSet<? extends StorageMetadata> list() {
	return activeInstance.list();
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public boolean containerExists(String container) {
	return activeInstance.containerExists(container);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public boolean createContainerInLocation(Location location, String container) {
	return activeInstance.createContainerInLocation(location, container);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public boolean createContainerInLocation(Location location,
	    String container, CreateContainerOptions options) {
	return activeInstance.createContainerInLocation(location, container,
		options);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public PageSet<? extends StorageMetadata> list(String container) {
	return activeInstance.list(container);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public PageSet<? extends StorageMetadata> list(String container,
	    ListContainerOptions options) {
	return activeInstance.list(container, options);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public void clearContainer(String container) {
	activeInstance.clearContainer(container);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public void clearContainer(String container, ListContainerOptions options) {
	activeInstance.clearContainer(container, options);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public void deleteContainer(String container) {
	activeInstance.deleteContainer(container);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public boolean directoryExists(String container, String directory) {
	return activeInstance.directoryExists(container, directory);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public void createDirectory(String container, String directory) {
	activeInstance.createDirectory(container, directory);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public void deleteDirectory(String containerName, String name) {
	activeInstance.deleteDirectory(containerName, name);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public String putBlob(String container, Blob blob, PutOptions options) {
	return activeInstance.putBlob(container, blob, options);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public BlobMetadata blobMetadata(String container, String name) {
	return activeInstance.blobMetadata(container, name);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public long countBlobs(String container) {
	return activeInstance.countBlobs(container);
    }

    /**
     * {@inheritDoc }
     */
    @Override
    public long countBlobs(String container, ListContainerOptions options) {
	return activeInstance.countBlobs(container, options);
    }
}
