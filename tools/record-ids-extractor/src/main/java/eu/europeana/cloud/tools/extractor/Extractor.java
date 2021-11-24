package eu.europeana.cloud.tools.extractor;

import com.google.common.base.Charsets;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.utils.RevisionIdentifier;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import lombok.extern.slf4j.Slf4j;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Slf4j
public class Extractor implements AutoCloseable {

    private static final int THREAD_COUNT = 100;
    private static final int REVISION_DATA_FETCH_SIZE = 1000;

    private final DataSetServiceClient dataSetServiceClient;
    private final UISClient uisClient;
    private final FileWriter writer;
    private final ExecutorService executor;
    private int rowCounter;

    public static void main(String[] args) throws Exception {
        enforceConfigFileValidation();
        if (args.length != 4) {
            System.out.println("Usage: java -jar extractor.jar <metis_dataset_id> <dataset_name> <revision_name> <revision_timestamp>");
            return;

        }
        String metisDatasetId = args[0];
        String datasetName = args[1];
        String revisionName = args[2];
        Instant revisionTimestamp;
        try {
            revisionTimestamp = Instant.parse(args[3]);
        } catch (DateTimeParseException e) {
            System.out.println("Can't parse revision timestamp: \"" + args[3] + "\"");
            return;
        }

        try (Extractor extractor = new Extractor()) {
            extractor.extractOneRevision(metisDatasetId, datasetName, new RevisionIdentifier(revisionName, Config.REVISION_PROVIDER, Date.from(revisionTimestamp)));
        }
    }

    public Extractor() throws IOException {
        executor = Executors.newFixedThreadPool(THREAD_COUNT);
        dataSetServiceClient = new DataSetServiceClient(Config.MCS_URL, Config.ECLOUD_USER, Config.ECLOUD_PASSWORD);
        uisClient = new UISClient(Config.UIS_URL, Config.ECLOUD_USER, Config.ECLOUD_PASSWORD);
        writer = new FileWriter("revision_ids.csv", Charsets.UTF_8);
        writeHeaderRow();
    }

    @Override
    public void close() throws Exception {
        writer.close();
        executor.shutdown();
        uisClient.close();
        dataSetServiceClient.close();
        log.info("Closed extractor. File completely flushed to disk.");
    }

    private void extractOneRevision(String metisDatasetId, String datasetName, RevisionIdentifier revision) throws MCSException, IOException, CloudException, ExecutionException, InterruptedException {
        log.info("Extracting ids from dataset: {} revision: {}", datasetName, revision);
        String startFrom = null;
        do {
            ResultSlice<CloudTagsResponse> slice = getCloudIdsChunk(datasetName, revision, startFrom);
            extractFromChunk(metisDatasetId, slice);
            startFrom = slice.getNextSlice();
        } while (startFrom != null);
    }

    private void extractFromChunk(String metisDatasetId, ResultSlice<CloudTagsResponse> slice) throws IOException, CloudException, ExecutionException, InterruptedException {
        log.debug("Start extracting from the chunk of {} records...", slice.getResults().size());
        List<Future<String>> rowsResults = prepareRowsInParallel(metisDatasetId, slice);
        writeRowsToFile(rowsResults);
        log.info("Extracted chunk of {} records, {} total, nextSlice: {}",
                slice.getResults().size(), rowCounter, slice.getNextSlice());
    }

    private List<Future<String>> prepareRowsInParallel(String metisDatasetId, ResultSlice<CloudTagsResponse> slice) {
        List<Future<String>> rowsResults = new ArrayList<>();
        for (CloudTagsResponse record : slice.getResults()) {
            rowsResults.add(executor.submit(() -> prepareOneRow(metisDatasetId, record)));
        }
        return rowsResults;
    }

    private void writeRowsToFile(List<Future<String>> rowsResults) throws IOException, InterruptedException, ExecutionException {
        for (Future<String> future : rowsResults) {
            writer.write(future.get());
            rowCounter++;
        }
    }

    private String prepareOneRow(String metisDatasetId, CloudTagsResponse record) throws CloudException {
        String cloudId = record.getCloudId();
        List<String> ids = getRecordIds(cloudId);

        String europeanaId = "";
        String localId = "";
        if (idsContainsRestrictedCharacters(ids)) {
            log.warn("For cloudId: {} found ids containing restricted characters ids: {}", cloudId, ids);
        } else if (ids.size() == 2) {
            List<String> europeanaIds = retainEuropeanaIds(metisDatasetId, ids);
            if (europeanaIds.size() == 1) {
                europeanaId = europeanaIds.get(0);
                localId = findRemainingId(ids, europeanaId);
            } else {
                log.warn("For cloudId: {} found europeanaIds: {}", cloudId, europeanaIds);
            }
        } else if (ids.size() == 1) {
            String id = ids.get(0);
            if (isEuropeanaId(metisDatasetId, id)) {
                europeanaId = id;
            } else {
                localId = id;
            }
        } else {
            log.warn("For cloudId: {} number of record ids: {}", cloudId, ids.size());
        }
        return createRow(cloudId, europeanaId, localId);
    }

    private void writeHeaderRow() throws IOException {
        writer.write("cloud_id,europeana_id,local_id\n");
    }

    private String createRow(String cloudId, String europeanaId, String localId) {
        return cloudId + "," + europeanaId + "," + localId + "\n";
    }

    private List<String> getRecordIds(String cloudId) throws CloudException {
        return RetryableMethodExecutor.executeOnRest("Error while getting Revisions from data set.",
                        () -> uisClient.getRecordId(cloudId).getResults())
                .stream().map(id -> id.getLocalId().getRecordId()).collect(Collectors.toList());
    }

    private ResultSlice<CloudTagsResponse> getCloudIdsChunk(
            String datasetName, RevisionIdentifier revision, String startFrom) throws MCSException {
        return RetryableMethodExecutor.executeOnRest("Error while getting Revisions from data set.",
                () -> dataSetServiceClient.getDataSetRevisionsChunk(Config.DATASET_PROVIDER, datasetName,
                        Config.REPRESENTATION_NAME, revision.getRevisionName(), revision.getRevisionProviderId(),
                        DateHelper.getISODateString(revision.getCreationTimeStamp()), startFrom, REVISION_DATA_FETCH_SIZE));
    }

    private boolean idsContainsRestrictedCharacters(List<String> ids) {
        return ids.stream().anyMatch(id -> id.contains(",") || id.contains("\n") || id.contains("\r"));
    }

    private List<String> retainEuropeanaIds(String metisDatasetId, List<String> localIds) {
        return localIds.stream().filter(id -> isEuropeanaId(metisDatasetId, id)).collect(Collectors.toList());
    }

    private String findRemainingId(List<String> ids, String europeanaId) {
        return ids.stream().filter(id -> !europeanaId.equals(id)).findFirst().get();
    }

    private boolean isEuropeanaId(String metisDatasetId, String id) {
        return id.startsWith(europeanaPrefix(metisDatasetId));
    }

    private String europeanaPrefix(String metisDatasetId) {
        return "/" + metisDatasetId + "/";
    }

    private static void enforceConfigFileValidation() {
        Config.MCS_URL.toString();
    }

}
