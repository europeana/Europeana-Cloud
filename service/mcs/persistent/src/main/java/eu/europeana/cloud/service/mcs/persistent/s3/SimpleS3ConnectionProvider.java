package eu.europeana.cloud.service.mcs.persistent.s3;

import jakarta.annotation.PreDestroy;
import java.util.NoSuchElementException;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage connection for S3 using jClouds library.
 */
public class SimpleS3ConnectionProvider implements S3ConnectionProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleS3ConnectionProvider.class);
  private BlobStoreContext context;
  private final String container;
  private BlobStore blobStore;

  private final String provider;
  private final String endpoint;
  private final String user;
  private final String password;

  /**
     * Class constructor. Establish connection to S3 endpoint using provided configuration.
   *
     * @param provider provider name. Pass "transient" if you want to use in-memory implementation for tests,
     *                and "s3" for accessing live S3 container.
     * @param container name of the S3 container (namespace)
     * @param endpoint s3 endpoint URL
   * @param user user identity
   * @param password user password
   */
    public SimpleS3ConnectionProvider(String provider, String container, String endpoint, String user, String password) {
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
        LOGGER.info("Connected to S3.");
  }

  @Override
  @PreDestroy
  public void closeConnections() {
        LOGGER.info("Shutting down S3 connection");
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
