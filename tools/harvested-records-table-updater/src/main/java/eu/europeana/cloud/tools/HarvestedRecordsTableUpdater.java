package eu.europeana.cloud.tools;

import com.datastax.driver.core.BatchStatement;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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

    /**
     * Possible lines covered by regular expression below
     * <ID>,<PATH>,<T1>,<T2>,<T3>?
     * "54","/54/item_5P7CNXVXZYWHHFO24XPSAJDQSA5DKLWP","1618222127555","1618222127555","1618222127555"
     * "54","/54/item_C5JL4OPQ2XBFG22HKLRNWLHLZIGBH3RR","1618222127555","1618222127555",
     * 54,/54/item_W2I5JU3M3EXM3HR6VV3YBH45SOEBMH2M,1618222127555,1618222127555,1618222127555
     * 54,/54/item_W7UDO6XLGZ73D2DYDT73P4AE4Y33HKBX,1618222127555,1618222127555,
     */
    private static final String LINE_PATTERN_REGEXP = "\\\"?(?<ID>.+?)\\\"?,\\\"?(?<PATH>.+?)\\\"?,\\\"?(?<T1>\\d+?)\\\"?,\\\"?(?<T2>\\d+?)\\\"?,(\\\"?(?<T3>\\d+?)\\\"?)?";
    private static final Pattern LINE_PATTERN = Pattern.compile(LINE_PATTERN_REGEXP);

    private static final int BATCH_SIZE = 10000;

    public static void main(String[] args) throws IOException {
        validateArgs(args);
        String fileLocation = args[0];
        String dbLocation = args[1];
        var dbPort = Integer.parseInt(args[2]);
        String keyspaceName = args[3];
        String userName = args[4];
        String password = args[5];

        LOGGER.info("Parsing file");
        List<HarvestedRecord> records = parseInputFile(fileLocation);
        LOGGER.info("Connecting database");
        var dbConnectionProvider = new CassandraConnectionProvider(
                dbLocation,
                dbPort,
                keyspaceName,
                userName,
                password);
        var dao = new HarvestedRecordsDAO(dbConnectionProvider);
        insertRecords(records, dbConnectionProvider, dao);
        dbConnectionProvider.closeConnections();
    }

    private static void insertRecords(List<HarvestedRecord> records, CassandraConnectionProvider dbConnectionProvider, HarvestedRecordsDAO dao) {
        LOGGER.info("Inserting records...");
        BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        records.forEach(harvestedRecord -> {
            batch.add(dao.insertBindParameters(harvestedRecord));
            LOGGER.info("Record inserted to batch: {}", harvestedRecord);
            if (batch.size() >= BATCH_SIZE) {
                dbConnectionProvider.getSession().execute(batch);
                LOGGER.info("Batch saved size: {}", BATCH_SIZE);
                batch.clear();
            }


        });
        if (batch.size() > 0) {
            dbConnectionProvider.getSession().execute(batch);
            LOGGER.info("Last Batch saved size: {}", batch.size());
        }
    }

    private static void validateArgs(String[] args) {
        if (args.length != 6) {
            LOGGER.info("HarvestedRecordsTableUpdater");
            LOGGER.info("Syntax:");
            LOGGER.info("\t\tjava -jar <your_jar_file_with_dependency> <csv_file> <db_host> <db_port> <db_keyspace> <db_username> <db_password>");
            System.exit(-1);
        }
    }

    private static List<HarvestedRecord> parseInputFile(String path) throws IOException {
        List<HarvestedRecord> recordsList = new ArrayList<>();

        try (var br = new BufferedReader(new FileReader(path))) {
            var line = "";
            while ((line = br.readLine()) != null) {

                Matcher lineMatcher = LINE_PATTERN.matcher(line);
                if(lineMatcher.matches()) {
                    var aRecord = HarvestedRecord.builder()
                            .metisDatasetId(lineMatcher.group("ID"))
                            .recordLocalId(lineMatcher.group("PATH"))
                            .publishedHarvestDate(new Date(Long.parseLong(lineMatcher.group("T1"))))
                            .previewHarvestDate(new Date(Long.parseLong(lineMatcher.group("T2"))))
                            .latestHarvestDate(
                                    lineMatcher.group("T3") != null ?
                                    new Date(Long.parseLong(lineMatcher.group("T3"))) :
                                    null)
                            .build();
                    recordsList.add(aRecord);
                } else {
                    LOGGER.warn("Non-data line in given file: |{}|", line);
                }
            }
        }
        return recordsList;
    }
}
