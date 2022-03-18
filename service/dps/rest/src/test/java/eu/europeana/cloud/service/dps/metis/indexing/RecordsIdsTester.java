package eu.europeana.cloud.service.dps.metis.indexing;

import com.datastax.driver.core.BatchStatement;
import com.google.common.base.Stopwatch;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.exit;


public class RecordsIdsTester extends PostprocessingSpeedTester{
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordsIdsTester.class);

    public static void main(String[] args) throws IndexingException {

        new RecordsIdsTester().make();
        exit(0);
    }

    protected void make() throws IndexingException {
        LOGGER.info("Started!");
        Date recordDate = DateHelper.parseISODate(RECORD_DATE_STRING);
        LOGGER.info("All count: {}", indexer.countRecords(METIS_DATASET_ID));
        LOGGER.info("Deleted count: {} " , indexer.countRecords(METIS_DATASET_ID,recordDate));
        doFullTraverse(recordDate, indexer);
    }
    
    private void doFullTraverse(Date recordDate, Indexer indexer) {
        Date latestHarvestDate=new Date();
        Stream<String> strame = indexer.getRecordIds(METIS_DATASET_ID, recordDate);
        AtomicInteger i=new AtomicInteger(1);

        strame.forEach(e->{
            System.out.print(".");
            if(i.get()%200==0){
                System.out.println();
            }
            if((i.get()%50)==0) {
                try {
                    LOGGER.info("Waiting....");
                    Thread.sleep(660_000L);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(e);
                }
            }
            i.incrementAndGet();
        });

    }



}
