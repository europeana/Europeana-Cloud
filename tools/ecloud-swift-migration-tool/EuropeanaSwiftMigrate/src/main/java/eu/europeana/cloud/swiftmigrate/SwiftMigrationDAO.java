package eu.europeana.cloud.swiftmigrate;

import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.s3.SimpleSwiftConnectionProvider;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;

import java.util.HashSet;
import java.util.Set;

/**
 * Class copy files from source container to target container.
 */
public class SwiftMigrationDAO {

  final SimpleSwiftConnectionProvider sourceProvider;
  final SimpleSwiftConnectionProvider targetProvider;


  public SwiftMigrationDAO(final SimpleSwiftConnectionProvider sourceConnectionProvider,
      final SimpleSwiftConnectionProvider targetConnectionProvider) {
    this.sourceProvider = sourceConnectionProvider;
    this.targetProvider = targetConnectionProvider;
  }


  /**
   * Metod copy single file from source container to target container.
   *
   * @param sourceObjectId source object name
   * @param trgObjectId target object name
   * @throws FileNotExistsException
   * @throws FileAlreadyExistsException
   */
  public void copyFile(final String sourceObjectId, final String trgObjectId)
      throws FileNotExistsException, FileAlreadyExistsException {
    final BlobStore blobStore = sourceProvider.getBlobStore();
    if (!blobStore.blobExists(sourceProvider.getContainer(), sourceObjectId)) {
      throw new FileNotExistsException(String.format("File %s not exists", sourceObjectId));
    }
    if (blobStore.blobExists(targetProvider.getContainer(), trgObjectId)) {
      throw new FileAlreadyExistsException(String.format("Target file %s already exists", trgObjectId));
    }
    final String container = targetProvider.getContainer();
    final Blob blob = blobStore.getBlob(sourceProvider.getContainer(), sourceObjectId);
    final Blob newBlob = blobStore.blobBuilder(trgObjectId).name(trgObjectId).payload(blob.getPayload()).build();
    blobStore.putBlob(container, newBlob);
  }


  /**
   * Method retrives list of files from source container.
   *
   * @return list of files
   */
  public Set<String> getFilesList() {
    final BlobStore blobStore = sourceProvider.getBlobStore();
    final String container = sourceProvider.getContainer();
    final Set<String> names = new HashSet<String>();
    PageSet<? extends StorageMetadata> ObjectList = blobStore.list(container);
    String marker = null;
    for (int i = 0; i < 500; i++) {

      for (StorageMetadata files : ObjectList) {
        names.add(files.getName());
      }
      marker = ObjectList.getNextMarker();
      if (marker == null) {
        break;
      }
      ObjectList = blobStore.list(container, ListContainerOptions.Builder.afterMarker(marker));
    }
    return names;
  }

}
