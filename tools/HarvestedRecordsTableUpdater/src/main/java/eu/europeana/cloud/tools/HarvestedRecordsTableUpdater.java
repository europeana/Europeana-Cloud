package eu.europeana.cloud.tools;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *  Script that will insert all relevant rows to the harvested_records table.
 *  Parameters has to be provided to this script in the following order:<br/>
 *
 * <b>file_location db_location db_port keyspace_name db_user_name db_password</b>
 */
public class HarvestedRecordsTableUpdater {

    private static final Logger LOGGER = LoggerFactory.getLogger(HarvestedRecordsTableUpdater.class);

    private static final String LINE_SEPARATOR = ",";

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
        LOGGER.info("Inserting records");
        records.forEach(harvestedRecord -> {
            dao.insertHarvestedRecord(harvestedRecord);
            LOGGER.info("Record inserted: {}",harvestedRecord);
        });
        dbConnectionProvider.closeConnections();
    }

    private static void validateArgs(String[] args){
        if (args.length != 6)
            throw new RuntimeException("Arguments are not valid");
    }

    private static List<HarvestedRecord> parseInputFile(String path) throws IOException {
        List<HarvestedRecord> records = new ArrayList<>();

        try (var br = new BufferedReader(new FileReader(path))) {
            var line = "";
            while ((line = br.readLine()) != null) {
                String[] lines = line.split(LINE_SEPARATOR);
                var record = HarvestedRecord.builder()
                        .metisDatasetId(lines[0])
                        .recordLocalId(lines[1])
                        .publishedHarvestDate(new Date(Instant.parse(lines[2]).toEpochMilli()))
                        .build();
                records.add(record);
            }
        }
        return records;
    }
}
