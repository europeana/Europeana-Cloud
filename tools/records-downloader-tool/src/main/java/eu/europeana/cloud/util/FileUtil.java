package eu.europeana.cloud.util;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * Created by Tarek on 9/1/2016.
 */
public class FileUtil {

  private static final int BATCH_MAX_SIZE = 1240 * 4;
  private static final String ZIP_FORMAT_EXTENSION = ".zip";
  private static final String ECLOUD_SUFFIX = "ecloud-dataset";

  public static void persistStreamToFile(InputStream inputStream, String folderPath, String fileName, String extension)
      throws IOException {
    OutputStream outputStream = null;
    try {
      String filePtah = createFilePath(folderPath, fileName, extension);
      File file = new File(filePtah);
      outputStream = new FileOutputStream(file.toPath().toString());
      byte[] buffer = new byte[BATCH_MAX_SIZE];
      IOUtils.copyLarge(inputStream, outputStream, buffer);
    } finally {
      if (outputStream != null) {
        outputStream.close();
      }
      inputStream.close();
    }

  }

  public static String createFilePath(String folderPath, String fileName, String extension) {
    String filePtah = folderPath + fileName;
    if ("".equals(FilenameUtils.getExtension(fileName))) {
      filePtah = filePtah + extension;
    }
    return filePtah;
  }


  public static String createFolder() throws IOException {
    String folderName = UUID.randomUUID().toString();
    return Files.createTempDirectory(folderName) + File.separator;

  }

  public static String createZipFolderPath(Date date) {
    String folderName = generateFolderName(date);
    return System.getProperty("user.dir") + "/" + folderName + ZIP_FORMAT_EXTENSION;
  }

  private static String generateFolderName(Date date) {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-ssss");
    return ECLOUD_SUFFIX + "-" + dateFormat.format(date);


  }


}
