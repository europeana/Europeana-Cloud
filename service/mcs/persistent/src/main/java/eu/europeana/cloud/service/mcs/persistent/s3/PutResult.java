package eu.europeana.cloud.service.mcs.persistent.s3;

import lombok.Getter;

/**
 * Control information about BLOB: number of bytes and md5 digest.
 */
@Getter
public class PutResult {

  private final String md5;

  private final Long contentLength;

  /**
   * Constucts PutResult with fiven number of bytes and md5 digest.
   *
   * @param md5 md5 digest
   * @param contentLength number of bytes
   */
  public PutResult(String md5, Long contentLength) {
    this.md5 = md5;
    this.contentLength = contentLength;
  }
}
