package eu.europeana.cloud.service.dps.storm.topologies.ic.topology.util;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.Files;

/**
 * Created by Tarek on 8/19/2016.
 */
public class ImageConverterUtil {
    private static final int BATCH_MAX_SIZE = 1240 * 4;

    public InputStream getStream(String filePath) throws IOException {
        File file = new File(filePath);
        return new FileInputStream(file);
    }

    public static String persistStreamToTemporaryStorage(ByteArrayInputStream inputStream, String fileName, String extension) throws IOException, RuntimeException {
        String folderPath = Files.createTempDirectory(fileName) + File.separator;
        File file = new File(folderPath + fileName + extension);

        try(OutputStream outputStream = new FileOutputStream(file.toPath().toString())) {
            byte[] buffer = new byte[BATCH_MAX_SIZE];
            IOUtils.copyLarge(inputStream, outputStream, buffer);
        } finally {
            inputStream.close();
        }
        return folderPath;
    }

    public static String buildFilePath(String folderPath, String fileName, String extension) {
        return folderPath + fileName + extension;
    }


}
