package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.Row;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntFunction;

public class BucketRecordTableIterator<T> extends BucketRecordIterator<T> {
    private Function<Row, T> convertMethod;
    private IntFunction<Iterator<Row>> bucketQueryMethod;

    protected BucketRecordTableIterator(int bucketCount,
                                        IntFunction<Iterator<Row>> bucketQueryMethod,
                                        RowConverter<T> convertMethod) {
        super(bucketCount);
        this.bucketQueryMethod = bucketQueryMethod;
        this.convertMethod = convertMethod;
    }

    @Override
    protected T convertRowToEntity(Row row) {
        return convertMethod.apply(row);
    }

    @Override
    protected Iterator<Row> queryBucket(int bucketNumber) {
        return Objects.requireNonNull(bucketQueryMethod.apply(bucketNumber));
    }

    public interface RowConverter<R> extends Function<Row, R> {
    }
}