package eu.europeana.cloud.downloader;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.util.FileUtil;
import eu.europeana.cloud.util.MimeTypeHelper;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeTypeException;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Created by Tarek on 9/14/2016.
 */
public class FileDownloaderJob implements Callable<Void> {

  private FileServiceClient fileServiceClient;
  private String folderPath;
  private Representation representation;
  private String fileName;
  private static final int MAX_RETRY_COUNT = 3;
  private static final int TIME_BETWEEN_RETRIES = 1000; //One second

  public FileDownloaderJob(FileServiceClient fileServiceClient, String fileName, Representation representation,
      String folderPath) {
    this.fileServiceClient = fileServiceClient;
    this.fileName = fileName;
    this.representation = representation;
    this.folderPath = folderPath;
  }

  @Override
  public final Void call()
      throws InterruptedException, ExecutionException, DriverException, MCSException, IOException, MimeTypeException {
    persistFileToFolderWithRetry(0, MAX_RETRY_COUNT);
    return null;
  }

  private void persistFileToFolder() throws MimeTypeException, DriverException, IOException, MCSException {
    final String fileUrl = fileServiceClient.getFileUri(representation.getCloudId(), representation.getRepresentationName(),
        representation.getVersion(), fileName).toString();
    InputStream inputStream = fileServiceClient.getFile(fileUrl);
    String extension = getExtension(inputStream);
    String persistedName = representation.getVersion() + "_" + fileName;
    FileUtil.persistStreamToFile(inputStream, folderPath, persistedName, extension);
  }

  private void persistFileToFolderWithRetry(int retryCount, int retryLimit)
      throws InterruptedException, MimeTypeException, IOException, MCSException {
    try {
      persistFileToFolder();
    } catch (DriverException | MCSException e) {
      if (retryCount > retryLimit) {
        throw e;
      }
      Thread.sleep(TIME_BETWEEN_RETRIES);
      persistFileToFolderWithRetry(++retryCount, retryLimit);
    }

  }

  private String getExtension(InputStream inputStream) throws MimeTypeException, IOException {
    MediaType mimeType = MimeTypeHelper.getMediaTypeFromStream(inputStream);
    return MimeTypeHelper.getExtension(mimeType);

  }

}