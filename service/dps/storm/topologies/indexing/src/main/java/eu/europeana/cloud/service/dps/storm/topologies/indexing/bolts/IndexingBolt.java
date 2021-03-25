package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import com.google.gson.Gson;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexingSettingsGenerator;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingEnvironment;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.DbConnectionDetails;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;

import java.io.Closeable;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by pwozniak on 4/6/18
 */
public class IndexingBolt extends AbstractDpsBolt {

    public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    public static final String PARSE_RECORD_DATE_ERROR_MESSAGE = "Could not parse RECORD_DATE parameter";
    public static final String INDEXING_FILE_ERROR_MESSAGE = "Unable to index file";
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingBolt.class);
    private static final int MAX_IDLE_TIME_FOR_INDEXER_IN_SECS = 600;
    private static final int IDLE_TIME_CHECK_INTERVAL_IN_SECS = 60;
    private transient IndexerPoolWrapper indexerPoolWrapper;
    private final DbConnectionDetails dbConnectionDetails;

    private final Properties indexingProperties;
    private transient HarvestedRecordsDAO harvestedRecordsDAO;
    private final String ecloudUisAddress;
    private transient UISClient uisClient;


    public IndexingBolt(DbConnectionDetails dbConnectionDetails,
                        Properties indexingProperties, String ecloudUisAddress) {
        this.dbConnectionDetails = dbConnectionDetails;
        this.indexingProperties = indexingProperties;
        this.ecloudUisAddress = ecloudUisAddress;
    }

    @Override
    protected boolean ignoreDeleted() {
        return false;
    }

    @Override
    public void prepare() {
        prepareDao();
        prepareUisClient();
        prepareIndexer();
    }

    @Override
    public void cleanup() {
        // This is just to close the connections in the pool and to prevent memory leaks.
        if (indexerPoolWrapper != null) {
            indexerPoolWrapper.close();
        }
        super.cleanup();
    }

    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        // Get variables.
        final String useAltEnv = stormTaskTuple
                .getParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV);
        final String datasetId = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
        final String database = stormTaskTuple
                .getParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE);
        final boolean preserveTimestampsString = Boolean
                .parseBoolean(stormTaskTuple.getParameter(PluginParameterKeys.METIS_PRESERVE_TIMESTAMPS));
        final String datasetIdsToRedirectFrom = stormTaskTuple
                .getParameter(PluginParameterKeys.DATASET_IDS_TO_REDIRECT_FROM);
        final List<String> datasetIdsToRedirectFromList = datasetIdsToRedirectFrom == null ? null
                : Arrays.asList(datasetIdsToRedirectFrom.trim().split("\\s*,\\s*"));
        final boolean performRedirects = Boolean
                .parseBoolean(stormTaskTuple.getParameter(PluginParameterKeys.PERFORM_REDIRECTS));
        DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT, Locale.US);
        final Date recordDate;
        try {
            recordDate = dateFormat
                .parse(stormTaskTuple.getParameter(PluginParameterKeys.METIS_RECORD_DATE));
            if (!stormTaskTuple.isMarkedAsDeleted()) {
                indexRecord(stormTaskTuple, useAltEnv, database, preserveTimestampsString, datasetIdsToRedirectFromList, performRedirects, recordDate);
                updateRecordHarvestingDates(stormTaskTuple);
            }
            prepareTuple(stormTaskTuple, useAltEnv, datasetId, database, recordDate);
            outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
            LOGGER.info(
                    "Indexing bolt executed for: {} (alternative environment: {}, record date: {}, preserve timestamps: {}).",
                    database, useAltEnv, recordDate, preserveTimestampsString);
        } catch (RuntimeException e) {
            logAndEmitError(anchorTuple, e, e.getMessage(), stormTaskTuple);
        } catch (ParseException e) {
            logAndEmitError(anchorTuple, e, PARSE_RECORD_DATE_ERROR_MESSAGE, stormTaskTuple);
        } catch (IndexingException e) {
            logAndEmitError(anchorTuple, e, INDEXING_FILE_ERROR_MESSAGE, stormTaskTuple);
        }
        outputCollector.ack(anchorTuple);
    }

    private void prepareDao() {
        CassandraConnectionProvider cassandraConnectionProvider =
                CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
                        dbConnectionDetails.getHosts(),
                        dbConnectionDetails.getPort(),
                        dbConnectionDetails.getKeyspaceName(),
                        dbConnectionDetails.getUserName(),
                        dbConnectionDetails.getPassword());
        harvestedRecordsDAO = new HarvestedRecordsDAO(cassandraConnectionProvider);
    }

    private void prepareUisClient() {
        uisClient = new UISClient(ecloudUisAddress);
    }

    private void prepareIndexer() {
        try {
            indexerPoolWrapper = new IndexerPoolWrapper(MAX_IDLE_TIME_FOR_INDEXER_IN_SECS,
                    IDLE_TIME_CHECK_INTERVAL_IN_SECS);
        } catch (IndexingException | URISyntaxException e) {
            LOGGER.error("Unable to initialize indexer", e);
            throw new RuntimeException(e);
        }
    }

    private void indexRecord(StormTaskTuple stormTaskTuple, String useAltEnv, String database, boolean preserveTimestampsString, List<String> datasetIdsToRedirectFromList, boolean performRedirects, Date recordDate) throws IndexingException {
        final IndexerPool indexerPool = indexerPoolWrapper.getIndexerPool(useAltEnv, database);

        final String document = new String(stormTaskTuple.getFileData());
        indexerPool.index(document, recordDate, preserveTimestampsString, datasetIdsToRedirectFromList, performRedirects);
    }

    private void prepareTuple(StormTaskTuple stormTaskTuple, String useAltEnv, String datasetId,
                              String database, Date recordDate) {
        stormTaskTuple.setFileData((byte[]) null);
        DataSetCleanerParameters dataSetCleanerParameters = new DataSetCleanerParameters(datasetId,
                Boolean.parseBoolean(useAltEnv), database, recordDate);
        stormTaskTuple.addParameter(PluginParameterKeys.DATA_SET_CLEANING_PARAMETERS,
                new Gson().toJson(dataSetCleanerParameters));
    }

    private void logAndEmitError(Tuple anchorTuple, Exception e, String errorMessage, StormTaskTuple stormTaskTuple) {
        LOGGER.error(errorMessage, e);
        emitErrorNotification(anchorTuple, stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), errorMessage,
                "Error while indexing. The full error is: " + ExceptionUtils.getStackTrace(e),
                StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
    }

    private void updateRecordHarvestingDates(StormTaskTuple stormTaskTuple) {
        try {
            String cloudIdentifier = extractCloudIdFromUrl(stormTaskTuple.getFileUrl());
            ResultSlice<CloudId> cloudIds = downloadCloudIdDefinition(cloudIdentifier);
            String metisDatasetId = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
            cloudIds.getResults()
                    .stream()
                    .filter(cloudId -> existsOnHarvestedRecordsList(cloudId, metisDatasetId))
                    .forEach(cloudId1 -> updateRecordIndexingDate(cloudId1, metisDatasetId));
        } catch (MalformedURLException | CloudException e) {
            LOGGER.error("Unable to update record harvesting dates");
            throw new RuntimeException("Unable to update record harvesting dates");
        }
    }

    private ResultSlice<CloudId> downloadCloudIdDefinition(String cloudIdentifier) throws CloudException {
        return uisClient.getRecordId(cloudIdentifier);
    }

    private String extractCloudIdFromUrl(String fileUrl) throws MalformedURLException {
        UrlParser parser = new UrlParser(fileUrl);
        return parser.getPart(UrlPart.RECORDS);
    }

    private boolean existsOnHarvestedRecordsList(CloudId cloudId, String metisDatasetId) {
        return harvestedRecordsDAO.findRecord(metisDatasetId, cloudId.getLocalId().getRecordId()).isPresent();
    }

    private void updateRecordIndexingDate(CloudId cloudId, String metisDatasetId) {
        LOGGER.info("Updating Indexing date for cloudId={}, metisDatasetId = {}", cloudId, metisDatasetId);
        harvestedRecordsDAO.updateIndexingDate(metisDatasetId, cloudId.getLocalId().getRecordId(), new Date());
    }

    class IndexerPoolWrapper implements Closeable {

        private IndexerPool indexerPoolForPreviewDbInDefaultEnv;
        private IndexerPool indexerPoolForPublishDbInDefaultEnv;

        private IndexerPool indexerPoolForPreviewDbInAnotherEnv;
        private IndexerPool indexerPoolForPublishDbInAnotherEnv;

        public IndexerPoolWrapper(long maxIdleTimeForIndexerInSecs,
                                  long idleTimeCheckIntervalInSecs) throws IndexingException, URISyntaxException {
            init(maxIdleTimeForIndexerInSecs, idleTimeCheckIntervalInSecs);
        }

        @Override
        public void close() {
            if (indexerPoolForPreviewDbInDefaultEnv != null) {
                indexerPoolForPreviewDbInDefaultEnv.close();
            }
            if (indexerPoolForPublishDbInDefaultEnv != null) {
                indexerPoolForPublishDbInDefaultEnv.close();
            }
            if (indexerPoolForPreviewDbInAnotherEnv != null) {
                indexerPoolForPreviewDbInAnotherEnv.close();
            }
            if (indexerPoolForPublishDbInAnotherEnv != null) {
                indexerPoolForPublishDbInAnotherEnv.close();
            }
        }

        private void init(long maxIdleTimeForIndexerInSecs, long idleTimeCheckIntervalInSecs)
                throws IndexingException, URISyntaxException {
            IndexingSettingsGenerator settingsGeneratorForDefaultEnv = new IndexingSettingsGenerator(
                    indexingProperties);
            IndexingSettingsGenerator settingsGeneratorForAnotherEnv = new IndexingSettingsGenerator(
                    TargetIndexingEnvironment.ALTERNATIVE, indexingProperties);

            IndexingSettings indexingSettingsForPreviewDbInDefaultEnv = settingsGeneratorForDefaultEnv
                    .generateForPreview();
            IndexingSettings indexingSettingsForPublishDbInDefaultEnv = settingsGeneratorForDefaultEnv
                    .generateForPublish();

            IndexingSettings indexingSettingsForPreviewDbInAnotherEnv = settingsGeneratorForAnotherEnv
                    .generateForPreview();
            IndexingSettings indexingSettingsForPublishDbInAnotherEnv = settingsGeneratorForAnotherEnv
                    .generateForPublish();

            indexerPoolForPreviewDbInDefaultEnv = initIndexerPool(
                    indexingSettingsForPreviewDbInDefaultEnv,
                    maxIdleTimeForIndexerInSecs, idleTimeCheckIntervalInSecs);
            indexerPoolForPublishDbInDefaultEnv = initIndexerPool(
                    indexingSettingsForPublishDbInDefaultEnv,
                    maxIdleTimeForIndexerInSecs, idleTimeCheckIntervalInSecs);

            if (indexingSettingsForPreviewDbInAnotherEnv != null) {
                indexerPoolForPreviewDbInAnotherEnv = initIndexerPool(
                        indexingSettingsForPreviewDbInAnotherEnv,
                        maxIdleTimeForIndexerInSecs, idleTimeCheckIntervalInSecs);
            }
            if (indexingSettingsForPublishDbInAnotherEnv != null) {
                indexerPoolForPublishDbInAnotherEnv = initIndexerPool(
                        indexingSettingsForPublishDbInAnotherEnv,
                        maxIdleTimeForIndexerInSecs, idleTimeCheckIntervalInSecs);
            }
        }

        private IndexerPool initIndexerPool(IndexingSettings indexingSettings,
                                            long maxIdleTimeForIndexerInSecs, long idleTimeCheckIntervalInSecs) {
            return new IndexerPool(indexingSettings, maxIdleTimeForIndexerInSecs,
                    idleTimeCheckIntervalInSecs);
        }

        IndexerPool getIndexerPool(String altEnv, String database) {
            if (Boolean.parseBoolean(altEnv)) {
                if (TargetIndexingDatabase.PREVIEW.toString().equals(database)) {
                    return indexerPoolForPreviewDbInAnotherEnv;
                } else if (TargetIndexingDatabase.PUBLISH.toString().equals(database)) {
                    return indexerPoolForPublishDbInAnotherEnv;
                }
            } else {
                if (TargetIndexingDatabase.PREVIEW.toString().equals(database)) {
                    return indexerPoolForPreviewDbInDefaultEnv;
                } else if (TargetIndexingDatabase.PUBLISH.toString().equals(database)) {
                    return indexerPoolForPublishDbInDefaultEnv;
                }
            }
            throw new RuntimeException("Specified environment and/or database is not recognized");
        }
    }
}
