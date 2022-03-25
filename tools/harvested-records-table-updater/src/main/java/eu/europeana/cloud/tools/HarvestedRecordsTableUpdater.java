package eu.europeana.cloud.tools;

import com.datastax.driver.core.BatchStatement;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.DbConnectionDetails;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Script that will insert all relevant rows to the harvested_records table.
 * Parameters has to be provided to this script in the following order:<br/>
 *
 * <b>file_location db_location db_port keyspace_name db_user_name db_password</b>
 */
public class HarvestedRecordsTableUpdater {

    private static final Logger LOGGER = LoggerFactory.getLogger(HarvestedRecordsTableUpdater.class);

    private static final String DATASET_ID_GROUP = "DSID";
    private static final String RECORD_ID_GROUP = "RECID";
    private static final String LATEST_GROUP = "TLATEST";
    private static final String PREVIEW_GROUP = "TPREVIEW";
    private static final String PUBLISHED_GROUP = "TPUBLISHED";
    /**
     * "metis_dataset_id","record_id","latest_harvest_revision_timestamp","preview_harvest_revision_timestamp","published_harvest_revision_timestamp"
     * Possible lines covered by regular expression below
     * <DSID>,<RECID>,<TLATEST>,<TPREVIEW>,<TPUBLISHED>
     * "54","/54/item_5P7CNXVXZYWHHFO24XPSAJDQSA5DKLWP","1618222127555","1618222127555","1618222127555"
     * "54","/54/item_C5JL4OPQ2XBFG22HKLRNWLHLZIGBH3RR","1618222127555","1618222127555",
     * 54,/54/item_W2I5JU3M3EXM3HR6VV3YBH45SOEBMH2M,1618222127555,1618222127555,1618222127555
     * 54,/54/item_W7UDO6XLGZ73D2DYDT73P4AE4Y33HKBX,1618222127555,1618222127555,
     */
    private static final String LINE_PATTERN_REGEXP = "\"?(?<"+DATASET_ID_GROUP+">.+?)\"?,\"?(?<"+RECORD_ID_GROUP+">.+?)\"?,\"?(?<"+LATEST_GROUP+">\\d*)\"?,\"?(?<"+PREVIEW_GROUP+">\\d*)\"?,\"?(?<"+PUBLISHED_GROUP+">\\d*)\"?";
    private static final Pattern LINE_PATTERN = Pattern.compile(LINE_PATTERN_REGEXP);

    private static final int BATCH_SIZE = 10000;

    private final CassandraConnectionProvider dbConnectionProvider;
    private final HarvestedRecordsDAO harvestedRecordsDAO;

    public HarvestedRecordsTableUpdater(DbConnectionDetails details){
        dbConnectionProvider = prepareConnectionProvider(details);
        harvestedRecordsDAO = new HarvestedRecordsDAO(dbConnectionProvider);
    }

    public static void main(String[] args) throws IOException {
        checkArgs(args);

        var thisApplication = new HarvestedRecordsTableUpdater(DbConnectionDetails
                .builder()
                .hosts(args[1])
                .port(Integer.parseInt(args[2]))
                .keyspaceName(args[3])
                .userName(args[4])
                .password(args[5])
                .build());
        thisApplication.run(args[0]);
    }

    private void run(String fileName) throws IOException {
        processFile(fileName);
        dbConnectionProvider.closeConnections();
    }

    private static void checkArgs(String[] args) {
        if (args.length != 6) {
            LOGGER.info("HarvestedRecordsTableUpdater");
            LOGGER.info("Syntax:");
            LOGGER.info("\t\tjava -jar <your_jar_file_with_dependency> <csv_file_path> <db_host> <db_port_integer> <db_keyspace> <db_username> <db_password>");
            System.exit(-1);
        }
    }

    private CassandraConnectionProvider prepareConnectionProvider(DbConnectionDetails dbConnectionDetails) {
        LOGGER.info("Connecting to database...");
        return new CassandraConnectionProvider(
                dbConnectionDetails.getHosts(),
                dbConnectionDetails.getPort(),
                dbConnectionDetails.getKeyspaceName(),
                dbConnectionDetails.getUserName(),
                dbConnectionDetails.getPassword());
    }

    private void processFile(String filename) throws IOException {
        LOGGER.info("Processing '{}' file", filename);

        var recordsList = new ArrayList<HarvestedRecord>();
        var batchNumber = 1;

        try (var lineNumberReader = new LineNumberReader(new FileReader(filename))) {
            var line = "";
            while ((line = lineNumberReader.readLine()) != null) {

                var lineMatcher = LINE_PATTERN.matcher(line);
                if(lineMatcher.matches()) {
                    recordsList.add(createRecord(lineMatcher));

                    if(recordsList.size() >= BATCH_SIZE) {
                        insertRecordsBatch(recordsList, batchNumber);
                        recordsList.clear();
                        batchNumber++;
                    }
                } else {
                    LOGGER.warn("Non-data line in given file: |{}|", line);
                }
            }

            insertRecordsBatch(recordsList, batchNumber);
        }
    }

    private HarvestedRecord createRecord(Matcher lineMatcher) {
        return HarvestedRecord.builder()
                .metisDatasetId(lineMatcher.group(DATASET_ID_GROUP))
                .recordLocalId(lineMatcher.group(RECORD_ID_GROUP))
                .latestHarvestDate(
                        lineMatcher.group(LATEST_GROUP).isEmpty() ? null :
                                new Date(Long.parseLong(lineMatcher.group(LATEST_GROUP)))
                )
                .previewHarvestDate(
                        lineMatcher.group(PREVIEW_GROUP).isEmpty() ? null :
                                new Date(Long.parseLong(lineMatcher.group(PREVIEW_GROUP)))
                )
                .publishedHarvestDate(lineMatcher.group(PUBLISHED_GROUP).isEmpty() ? null :
                        new Date(Long.parseLong(lineMatcher.group(PUBLISHED_GROUP))))
                .build();
    }

    private void insertRecordsBatch(List<HarvestedRecord> recordsList, int batchNumber) {
        LOGGER.info("Inserting {} records to batch number {} ...", recordsList.size(), batchNumber);
        var batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        recordsList.forEach(harvestedRecord -> {
            batch.add(harvestedRecordsDAO.prepareInsertStatement(harvestedRecord));
            LOGGER.debug("Record inserted to batch: {}", harvestedRecord);
        });

        dbConnectionProvider.getSession().execute(batch);
        LOGGER.info("Batch {} saved. Batch size: {}", batchNumber, batch.size());
        batch.clear();
    }
}
