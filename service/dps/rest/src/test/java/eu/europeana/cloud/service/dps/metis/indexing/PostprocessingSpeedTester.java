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
import java.util.stream.Collectors;

import static java.lang.System.exit;


public class PostprocessingSpeedTester {

    protected static final String RECORD_DATE_STRING = "2022-03-03T10:55:26.127Z";

    protected static final String METIS_DATASET_ID = "425";
    private static final Logger LOGGER = LoggerFactory.getLogger(PostprocessingSpeedTester.class);
    protected static final int BATCH_SIZE = 1000;
    protected final HarvestedRecordsDAO dao;
    protected final CassandraConnectionProvider provider;
    protected final Indexer indexer;


    public static void main(String[] args) throws IndexingException {

        new PostprocessingSpeedTester().make();
        exit(0);
    }

    public PostprocessingSpeedTester(){
        dao = IndexWrapperInstance.getHarvestedRecordsDAO();
        provider = IndexWrapperInstance.getProvider();
        LOGGER.info("Created cassandra!");
        IndexWrapper w= IndexWrapperInstance.getWrapper();
        indexer = w.getIndexer(TargetIndexingDatabase.PREVIEW);
        LOGGER.info("Created indexer!");

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
        List<String> list = indexer.getRecordIds(METIS_DATASET_ID, recordDate)
                .sorted(Comparator.comparing(dao::bucketNoFor))
                .collect(Collectors.toList());
        LOGGER.info("Rows ready to insert count: {} " , list.size());
        int i = 0;
        Stopwatch w=Stopwatch.createStarted();
        BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        int lastBucket=-1;
        for (String id : list) {
            int currentBucket = dao.bucketNoFor(id);
            if (lastBucket != currentBucket) {
                saveBatch(i, batch);
            }

            batch.add(dao.prepareUpdatePreviewColumnsForExisting(METIS_DATASET_ID, id, latestHarvestDate, UUID.randomUUID()));
            lastBucket=currentBucket;

            if(batch.size()>=BATCH_SIZE){
                saveBatch(i, batch);
            }

            i++;
        }

        saveBatch(i,batch);
        LOGGER.info("Deleted list size: {} time: {}", list.size(),w.elapsed());

    }

    private void saveBatch(int i, BatchStatement batch) {
        if(batch.size()>0) {
            provider.getSession().execute(batch);
            LOGGER.info("Saved batch record: {}, batch size: {} ", i, batch.size());
            batch.clear();
        }
    }


}
