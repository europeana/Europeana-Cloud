package eu.europeana.cloud.service.mcs.persistent.s3;

import org.jclouds.blobstore.BlobStore;

/**
 * Manage connection for S3 endpoints using jClouds library.
 */
public interface S3ConnectionProvider {

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
  void closeConnections();

}
