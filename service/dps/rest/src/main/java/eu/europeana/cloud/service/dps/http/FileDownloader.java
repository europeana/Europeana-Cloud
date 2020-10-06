package eu.europeana.cloud.service.dps.http;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileDownloader {

    private static final int BATCH_MAX_SIZE = 1240 * 4;

    public File downloadFile(String fileLocation, String fileDestination) throws IOException {

        File file = prepareDirectory(fileDestination, fileLocation);
        byte[] buffer = new byte[BATCH_MAX_SIZE];

        URL url = new URL(fileLocation);
        URLConnection conn = url.openConnection();

        try (InputStream inputStream = conn.getInputStream();
             OutputStream outputStream = new FileOutputStream(file)) {
            IOUtils.copyLarge(inputStream, outputStream, buffer);
            return file;
        }
    }

    private File prepareDirectory(String fileDestination, String fileLocation) throws IOException {
        Path path = Files.createDirectory(Paths.get(fileDestination));
        return new File(path.toString(), FilenameUtils.getName(fileLocation));
    }
}
