package eu.europeana.cloud.service.mcs.persistent.s3;

import software.amazon.awssdk.services.s3.S3Client;

/**
 * Manage connection for S3 endpoints using jClouds library.
 */
public interface S3ConnectionProvider {

  /**
   * @return {@link S3Client}
   */
  S3Client getS3Client();


  /**
   * @return name of container
   */
  String getContainer();


  /**
   * Close connection on container destroy.
   */
  void closeConnections();

}
