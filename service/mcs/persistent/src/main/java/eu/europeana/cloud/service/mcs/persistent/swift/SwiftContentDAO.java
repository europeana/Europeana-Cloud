package eu.europeana.cloud.service.mcs.persistent.swift;

import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.io.Payload;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * Provides DAO operations for Openstack Swift.
 */
@Repository
public class SwiftContentDAO implements ContentDAO {

  private static final String MSG_FILE_NOT_EXISTS = "File %s not exists";
  private static final String MSG_TARGET_FILE_ALREADY_EXISTS = "Target file %s already exists";
  private static final String MSG_CANNOT_GET_INSTANCE_OF_MD_5 = "Cannot get instance of MD5 but such algorithm should be provided";

  @Autowired
  private SwiftConnectionProvider connectionProvider;

  @Override
  public PutResult putContent(String fileName, InputStream data) throws IOException, ContainerNotFoundException {
    BlobStore blobStore = connectionProvider.getBlobStore();
    String container = connectionProvider.getContainer();
    CountingInputStream countingInputStream = new CountingInputStream(data);
    DigestInputStream md5DigestInputStream = md5InputStream(countingInputStream);
    BlobBuilder builder = blobStore.blobBuilder(fileName);
    builder = builder.name(fileName);
    builder = builder.payload(md5DigestInputStream);
    Blob blob = builder.build();
    blobStore.putBlob(container, blob);

    String md5 = BaseEncoding.base16().lowerCase().encode(md5DigestInputStream.getMessageDigest().digest());
    Long contentLength = countingInputStream.getCount();
    return new PutResult(md5, contentLength);
  }

  @Override
  public void getContent(String fileName, long start, long end, OutputStream os)
      throws IOException, FileNotExistsException, ContainerNotFoundException {
    BlobStore blobStore = connectionProvider.getBlobStore();
    if (!blobStore.blobExists(connectionProvider.getContainer(), fileName)) {
      throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, fileName));
    }

    GetOptions options = GetOptions.NONE;
    if (start > -1 && end > -1) {
      options = new GetOptions().range(start, end);
    } else if (start > -1 && end == -1) {
      options = new GetOptions().startAt(start);
    } else if (start == -1 && end > -1) {
      options = new GetOptions().range(0, end);
    }
    String container = connectionProvider.getContainer();
    Payload payload = blobStore.getBlob(container, fileName, options).getPayload();
    if (payload != null) {
      ByteStreams.copy(payload.openStream(), os);
    }
  }

  @Override
  public void copyContent(String sourceObjectId, String trgObjectId)
      throws FileNotExistsException, FileAlreadyExistsException {
    BlobStore blobStore = connectionProvider.getBlobStore();
    if (!blobStore.blobExists(connectionProvider.getContainer(), sourceObjectId)) {
      throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, sourceObjectId));
    }
    if (blobStore.blobExists(connectionProvider.getContainer(), trgObjectId)) {
      throw new FileAlreadyExistsException(String.format(MSG_TARGET_FILE_ALREADY_EXISTS, trgObjectId));
    }
    String container = connectionProvider.getContainer();
    Blob blob = blobStore.getBlob(container, sourceObjectId);
    Blob newBlob = blobStore.blobBuilder(trgObjectId).name(trgObjectId).payload(blob.getPayload()).build();
    blobStore.putBlob(container, newBlob);
  }

  @Override
  public void deleteContent(String fileName)
      throws FileNotExistsException {
    BlobStore blobStore = connectionProvider.getBlobStore();
    if (!blobStore.blobExists(connectionProvider.getContainer(), fileName)) {
      throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, fileName));
    }
    String container = connectionProvider.getContainer();
    blobStore.removeBlob(container, fileName);
  }

  private DigestInputStream md5InputStream(InputStream is) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      return new DigestInputStream(is, md);
    } catch (NoSuchAlgorithmException ex) {
      throw new AssertionError(MSG_CANNOT_GET_INSTANCE_OF_MD_5, ex);
    }
  }
}
