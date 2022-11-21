package eu.europeana.cloud.http.common;

import java.util.ArrayList;
import java.util.List;

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
    List<String> values = new ArrayList<>(CompressionFileExtension.values().length);
    for (CompressionFileExtension extension : CompressionFileExtension.values()) {
      values.add(extension.getExtension());
    }
    String[] arrayOfValues = new String[values.size()];
    return values.toArray(arrayOfValues);

  }


}
