package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.Row;

import java.util.Iterator;

public abstract class BucketRecordIterator<T> implements Iterator<T> {
    private final int bucketCount;
    private int bucketNumber = -1;
    private Iterator<Row> currentBucketIterator;

    protected BucketRecordIterator(int bucketCount) {
        this.bucketCount = bucketCount;
    }

    @Override
    public boolean hasNext() {
        goToNextBucketIfNeeded();
        return currentBucketIterator.hasNext();
    }

    @Override
    public T next() {
        goToNextBucketIfNeeded();
        return convertRowToEntity(currentBucketIterator.next());
    }

    private void goToNextBucketIfNeeded() {
        while ((currentBucketIterator == null) || (!currentBucketIterator.hasNext() && notLastBucket())) {
            bucketNumber++;
            currentBucketIterator = queryBucket(bucketNumber);
        }
    }

    protected abstract T convertRowToEntity(Row row);

    protected abstract Iterator<Row> queryBucket(int bucketNumber);

    private boolean notLastBucket() {
        return bucketNumber < (bucketCount - 1);
    }
}