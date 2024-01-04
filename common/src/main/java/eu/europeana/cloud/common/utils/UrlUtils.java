package eu.europeana.cloud.common.utils;

/**
 * @author krystian.
 */
public final class UrlUtils {

  private UrlUtils() {
  }

  public static String removeLastSlash(final String url) {
    if (url.endsWith("/")) {
      return url.substring(0, url.length() - 1);
    } else {
      return url;
    }

  }
}
