package eu.europeana.cloud.service.dps.metis.indexing;

import com.mongodb.client.internal.MongoBatchCursorAdapter;
import dev.morphia.query.MorphiaCursor;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.lang.System.exit;


public class FetchTester extends PostprocessingSpeedTester {
    private static final Logger LOGGER = LoggerFactory.getLogger(FetchTester.class);

    public static void main(String[] args) throws IndexingException {
        new FetchTester().make();
        exit(0);
    }

    protected void make() throws IndexingException {
        LOGGER.info("Started!");
        Date recordDate = DateHelper.parseISODate(RECORD_DATE_STRING);
        LOGGER.info("All count: {}", indexer.countRecords(METIS_DATASET_ID));
        LOGGER.info("Deleted count: {} ", indexer.countRecords(METIS_DATASET_ID, recordDate));
        doFullTraverse(recordDate, indexer);
    }

    private void doFullTraverse(Date recordDate, Indexer indexer) {
        final Stream<String> strame = indexer.getRecordIds(METIS_DATASET_ID, recordDate);
        AtomicInteger i = new AtomicInteger(1);

        MorphiaCursor cursor = extractCursor(strame);

        strame.forEach(e -> {
//            System.out.print(".");
//            if (i.get() % 200 == 0) {
//                System.out.println();
//            }
            MongoBatchCursorAdapter wrapped = extractWrapped(cursor);

            ArrayList curBatch = getCurBatch(wrapped);
            LOGGER.info("Stream element: {} cached size: {}",i.get(), curBatch!=null?curBatch.size():null);

            i.incrementAndGet();
        });

    }

    private ArrayList getCurBatch(MongoBatchCursorAdapter cursor) {
        try {
            Field curBatch = cursor.getClass().getDeclaredField("curBatch");
            curBatch.setAccessible(true);
            return (ArrayList) curBatch.get(cursor);
        } catch (NoSuchFieldException | IllegalAccessException  e) {
            throw new RuntimeException(e);
        }
    }

    private MongoBatchCursorAdapter extractWrapped(MorphiaCursor cursor) {
        try {
            Field wrappedField = MorphiaCursor.class.getDeclaredField("wrapped");
            wrappedField.setAccessible(true);
            return (MongoBatchCursorAdapter) wrappedField.get(cursor);
        } catch (NoSuchFieldException | IllegalAccessException  e) {
            throw new RuntimeException(e);
        }
    }

    private MorphiaCursor extractCursor(Stream<String> strame) {
        try {
            Field sourceStageField = strame.getClass().getDeclaredField("this$0");
            sourceStageField.setAccessible(true);
            Object sourceStage = sourceStageField.get(strame);
            Field sourceIteratorField = Class.forName("java.util.stream.AbstractPipeline").getDeclaredField("sourceSpliterator");
            sourceIteratorField.setAccessible(true);
            Object sourceIterator = sourceIteratorField.get(sourceStage);
            Field cursorField = sourceIterator.getClass().getDeclaredField("it");


            cursorField.setAccessible(true);
            MorphiaCursor cursor = (MorphiaCursor) cursorField.get(sourceIterator);
            return cursor;
        } catch (NoSuchFieldException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


}
