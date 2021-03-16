package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.Row;

import java.util.Iterator;

public abstract class BucketRecordIterator<T> implements Iterator<T> {
    private int bucketNo = -1;
    private Iterator<Row> currentBucketIterator;

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
        if(currentBucketIterator==null){
            currentBucketIterator=queryBucket(bucketNo);
        }
        while ((currentBucketIterator==null) || (!currentBucketIterator.hasNext() && notLastBucket())) {
            bucketNo++;
            currentBucketIterator=queryBucket(bucketNo);
        }
    }

    protected abstract T convertRowToEntity(Row row);

    protected abstract Iterator<Row> queryBucket(int bucketNumber);

    private boolean notLastBucket() {
        return bucketNo < (HarvestedRecordDAO.OAI_ID_BUCKET_COUNT - 1);
    }


}
