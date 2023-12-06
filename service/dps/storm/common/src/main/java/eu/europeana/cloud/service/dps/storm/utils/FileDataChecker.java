package eu.europeana.cloud.service.dps.storm.utils;

import java.nio.charset.StandardCharsets;

public final class FileDataChecker {

    private FileDataChecker() {
    }

    public static boolean isFileDataNullOrBlank(byte[] fileData) {
        if (fileData == null) {
            return true;
        }
        String fileContent = new String(fileData, StandardCharsets.UTF_8);
        return fileContent.isBlank();
    }
}
