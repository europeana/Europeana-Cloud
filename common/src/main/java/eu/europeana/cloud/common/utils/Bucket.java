package eu.europeana.cloud.common.utils;

public class Bucket {

  private String objectId;

  private String bucketId;

  private long rowsCount;

  public Bucket(String bucketId, long rowsCount) {
    this.bucketId = bucketId;
    this.rowsCount = rowsCount;
  }

  public Bucket(String objectId, String bucketId, long rowsCount) {
    this.objectId = objectId;
    this.bucketId = bucketId;
    this.rowsCount = rowsCount;
  }

  public String getBucketId() {
    return bucketId;
  }

  public long getRowsCount() {
    return rowsCount;
  }

  public String getObjectId() {
    return objectId;
  }
}
