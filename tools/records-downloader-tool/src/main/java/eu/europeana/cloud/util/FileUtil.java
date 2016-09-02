package eu.europeana.cloud.util;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.Files;
import java.util.UUID;

/**
 * Created by Tarek on 9/1/2016.
 */
public class FileUtil {
    private static final int BATCH_MAX_SIZE = 1240 * 4;

    public static void persistStreamToFile(InputStream inputStream, String folderPath, String fileName, String extension) throws IOException, RuntimeException {
        OutputStream outputStream = null;
        try {
            File file = new File(folderPath + fileName + extension);
            outputStream = new FileOutputStream(file.toPath().toString());
            byte[] buffer = new byte[BATCH_MAX_SIZE];
            IOUtils.copyLarge(inputStream, outputStream, buffer);
        } finally {
            if (outputStream != null)
                outputStream.close();
            inputStream.close();
        }

    }

    public static String buildPath(String folderPath, String fileName, String extension) {
        return folderPath + "/" + fileName + extension;
    }

    public static String createFolder() throws IOException {
        String folderName = UUID.randomUUID().toString();
        String folderPath = Files.createTempDirectory(folderName) + File.separator;
        return folderPath;
    }
}
