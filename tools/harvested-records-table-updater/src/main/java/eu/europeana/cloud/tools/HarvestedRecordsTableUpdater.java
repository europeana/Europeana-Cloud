package eu.europeana.cloud.tools;

import com.datastax.driver.core.BatchStatement;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Script that will insert all relevant rows to the harvested_records table.
 * Parameters has to be provided to this script in the following order:<br/>
 *
 * <b>file_location db_location db_port keyspace_name db_user_name db_password</b>
 */
public class HarvestedRecordsTableUpdater {

    private static final Logger LOGGER = LoggerFactory.getLogger(HarvestedRecordsTableUpdater.class);

    /**
     * Possible lines covered by regular expression below
     * <DSID>,<RECID>,<T1>,<T2>,<T3>?
     * "54","/54/item_5P7CNXVXZYWHHFO24XPSAJDQSA5DKLWP","1618222127555","1618222127555","1618222127555"
     * "54","/54/item_C5JL4OPQ2XBFG22HKLRNWLHLZIGBH3RR","1618222127555","1618222127555",
     * 54,/54/item_W2I5JU3M3EXM3HR6VV3YBH45SOEBMH2M,1618222127555,1618222127555,1618222127555
     * 54,/54/item_W7UDO6XLGZ73D2DYDT73P4AE4Y33HKBX,1618222127555,1618222127555,
     */
    private static final String LINE_PATTERN_REGEXP = "\\\"?(?<DSID>.+?)\\\"?,\\\"?(?<RECID>.+?)\\\"?,\\\"?(?<T1>\\d+?)\\\"?,\\\"?(?<T2>\\d+?)\\\"?,(\\\"?(?<T3>\\d+?)\\\"?)?";
    private static final Pattern LINE_PATTERN = Pattern.compile(LINE_PATTERN_REGEXP);

    private static final int BATCH_SIZE = 10000;

    private CassandraConnectionProvider dbConnectionProvider;
    private HarvestedRecordsDAO harvestedRecordsDAO;

    public static void main(String[] args) throws IOException {
        var thisApplication = new HarvestedRecordsTableUpdater();
        thisApplication.run(args);
    }

    private void run(String[] args) throws IOException {
        checkArgs(args);

        dbConnectionProvider = prepareConnectionProvider(args);
        harvestedRecordsDAO = new HarvestedRecordsDAO(dbConnectionProvider);

        processFile(args[0]);

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

    private CassandraConnectionProvider prepareConnectionProvider(String[] args) {
        LOGGER.info("Connecting to database...");
        return new CassandraConnectionProvider(args[1], Integer.parseInt(args[2]), args[3], args[4], args[5]);
    }

    private void processFile(String filename) throws IOException {
        LOGGER.info("Processing '{}' file", filename);

        var recordsList = new ArrayList<HarvestedRecord>();
        var batachCounter = 1;

        try (var lineNumberReader = new LineNumberReader(new FileReader(filename))) {
            var line = "";
            while ((line = lineNumberReader.readLine()) != null) {

                var lineMatcher = LINE_PATTERN.matcher(line);
                if(lineMatcher.matches()) {
                    var aRecord = HarvestedRecord.builder()
                            .metisDatasetId(lineMatcher.group("DSID"))
                            .recordLocalId(lineMatcher.group("RECID"))
                            .publishedHarvestDate(new Date(Long.parseLong(lineMatcher.group("T1"))))
                            .previewHarvestDate(new Date(Long.parseLong(lineMatcher.group("T2"))))
                            .latestHarvestDate(lineMatcher.group("T3") != null ?
                                            new Date(Long.parseLong(lineMatcher.group("T3"))) : null)
                            .build();
                    recordsList.add(aRecord);

                    if(recordsList.size() >= BATCH_SIZE) {
                        insertRecordsBatch(recordsList, batachCounter);
                        recordsList.clear();
                        batachCounter++;
                    }
                } else {
                    LOGGER.warn("Non-data line in given file: |{}|", line);
                }
            }

            insertRecordsBatch(recordsList, batachCounter);
        }
    }

    private void insertRecordsBatch(List<HarvestedRecord> records, int batchCounter) {
        LOGGER.info("Inserting {} records to batch number {} ...", records.size(), batchCounter);
        var batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        records.forEach(harvestedRecord -> {
            batch.add(harvestedRecordsDAO.insertBindParameters(harvestedRecord));
            LOGGER.debug("Record inserted to batch: {}", harvestedRecord);
        });

        dbConnectionProvider.getSession().execute(batch);
        LOGGER.info("Batch {} saved. Batch size: {}", batchCounter, batch.size());
        batch.clear();
    }
}
