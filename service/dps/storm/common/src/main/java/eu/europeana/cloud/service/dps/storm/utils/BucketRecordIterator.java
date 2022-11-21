package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.Row;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntFunction;

public class BucketRecordIterator<T> implements Iterator<T> {

  private final int bucketCount;
  private final Function<Row, T> convertMethod;
  private final IntFunction<Iterator<Row>> bucketQueryMethod;

  private int bucketNumber = -1;
  private Iterator<Row> currentBucketIterator;

  public BucketRecordIterator(int bucketCount,
      IntFunction<Iterator<Row>> bucketQueryMethod,
      RowConverter<T> convertMethod) {
    this.bucketCount = bucketCount;
    this.bucketQueryMethod = bucketQueryMethod;
    this.convertMethod = convertMethod;
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

  protected T convertRowToEntity(Row row) {
    return convertMethod.apply(row);
  }

  protected Iterator<Row> queryBucket(int bucketNumber) {
    return Objects.requireNonNull(bucketQueryMethod.apply(bucketNumber));
  }

  private boolean notLastBucket() {
    return bucketNumber < (bucketCount - 1);
  }

  public interface RowConverter<R> extends Function<Row, R> {

  }

}
