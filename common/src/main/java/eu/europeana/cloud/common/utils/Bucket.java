package eu.europeana.cloud.common.utils;

public class Bucket {
    private String bucketId;

    private long rowsCount;

    public Bucket(String bucketId, long rowsCount) {
        this.bucketId = bucketId;
        this.rowsCount = rowsCount;
    }

    public String getBucketId() {
        return bucketId;
    }

    public long getRowsCount() {
        return rowsCount;
    }
}
