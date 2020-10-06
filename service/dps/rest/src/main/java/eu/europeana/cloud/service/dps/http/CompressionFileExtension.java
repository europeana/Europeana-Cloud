package eu.europeana.cloud.service.dps.http;

import java.util.Arrays;

public enum CompressionFileExtension {
    ZIP("zip"),
    GZIP("gz"),
    TGZIP("tgz");

    private final String extension;


    CompressionFileExtension(String extension) {
        this.extension = extension;
    }

    public final String getExtension() {
        return extension;
    }

    public static boolean contains(String fileExtension) {
        for (CompressionFileExtension extension : CompressionFileExtension.values()) {
            if (extension.getExtension().equals(fileExtension)) {
                return true;
            }
        }
        return false;
    }

    public static String[] getExtensionValues() {
        return Arrays.stream(values())
                .map(CompressionFileExtension::getExtension)
                .toArray(String[]::new);
    }

}
