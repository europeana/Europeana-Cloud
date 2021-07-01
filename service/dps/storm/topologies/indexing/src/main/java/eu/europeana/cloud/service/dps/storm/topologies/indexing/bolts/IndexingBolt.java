package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import com.google.gson.Gson;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexingSettingsGenerator;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingEnvironment;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.TopologyGeneralException;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.DbConnectionDetails;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingProperties;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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
    private final String uisAddress;
    private transient EuropeanaIdFinder europeanaIdFinder;


    public IndexingBolt(DbConnectionDetails dbConnectionDetails,
                        Properties indexingProperties, String uisAddress) {
        this.dbConnectionDetails = dbConnectionDetails;
        this.indexingProperties = indexingProperties;
        this.uisAddress = uisAddress;
    }

    @Override
    protected boolean ignoreDeleted() {
        return false;
    }

    @Override
    public void prepare() {
        prepareDao();
        prepareEuropeanaIdFinder();
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
        final var useAltEnv = stormTaskTuple
                .getParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV);
        final var datasetId = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
        final var database = stormTaskTuple
                .getParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE);
        final var preserveTimestampsString = Boolean
                .parseBoolean(stormTaskTuple.getParameter(PluginParameterKeys.METIS_PRESERVE_TIMESTAMPS));
        final var datasetIdsToRedirectFrom = stormTaskTuple
                .getParameter(PluginParameterKeys.DATASET_IDS_TO_REDIRECT_FROM);
        final var datasetIdsToRedirectFromList = datasetIdsToRedirectFrom == null ? null
                : Arrays.asList(datasetIdsToRedirectFrom.trim().split("\\s*,\\s*"));
        final var performRedirects = Boolean
                .parseBoolean(stormTaskTuple.getParameter(PluginParameterKeys.PERFORM_REDIRECTS));
        DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT, Locale.US);
        final Date recordDate;
        try {
            recordDate = dateFormat
                    .parse(stormTaskTuple.getParameter(PluginParameterKeys.METIS_RECORD_DATE));
            final var properties = new IndexingProperties(recordDate,
                    preserveTimestampsString, datasetIdsToRedirectFromList, performRedirects, true);

            String metisDatasetId = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
            String europeanaId = europeanaIdFinder.findForFileUrl(metisDatasetId, stormTaskTuple.getFileUrl());
            if (!stormTaskTuple.isMarkedAsDeleted()) {
                indexRecord(stormTaskTuple, useAltEnv, database, properties);
            } else{
                removeIndexedRecord(stormTaskTuple, useAltEnv, database, europeanaId);
            }
            findAndUpdateHarvestedRecord(stormTaskTuple, europeanaId);

            prepareTuple(stormTaskTuple, useAltEnv, datasetId, database, recordDate);
            outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
            LOGGER.info(
                    "Indexing bolt executed for: {} (alternative environment: {}, record date: {}, preserve timestamps: {}).",
                    database, useAltEnv, recordDate, preserveTimestampsString);
        } catch (RuntimeException | MalformedURLException | CloudException e) {
            logAndEmitError(anchorTuple, e, e.getMessage(), stormTaskTuple);
        } catch (ParseException e) {
            logAndEmitError(anchorTuple, e, PARSE_RECORD_DATE_ERROR_MESSAGE, stormTaskTuple);
        } catch (IndexingException e) {
            logAndEmitError(anchorTuple, e, INDEXING_FILE_ERROR_MESSAGE, stormTaskTuple);
        }
        outputCollector.ack(anchorTuple);
    }

    private void removeIndexedRecord(StormTaskTuple stormTaskTuple, String useAltEnv, String database, String europeanaId) throws IndexingException {
        LOGGER.info("Removing indexed record europeanaId: {}, database: {} useAltEnv: {}, taskId: {}, recordId: {}",
                europeanaId, database, useAltEnv, stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl());
        indexerPoolWrapper.getIndexerPool(useAltEnv, database).remove(europeanaId);
    }

    private void prepareDao() {
        var cassandraConnectionProvider =
                CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
                        dbConnectionDetails.getHosts(),
                        dbConnectionDetails.getPort(),
                        dbConnectionDetails.getKeyspaceName(),
                        dbConnectionDetails.getUserName(),
                        dbConnectionDetails.getPassword());
        harvestedRecordsDAO = HarvestedRecordsDAO.getInstance(cassandraConnectionProvider);
    }

    private void prepareEuropeanaIdFinder() {
        europeanaIdFinder = new EuropeanaIdFinder(new UISClient(uisAddress), harvestedRecordsDAO);
    }

    private void prepareIndexer() {
        try {
            indexerPoolWrapper = new IndexerPoolWrapper(MAX_IDLE_TIME_FOR_INDEXER_IN_SECS,
                    IDLE_TIME_CHECK_INTERVAL_IN_SECS);
        } catch (IndexingException | URISyntaxException e) {
            var message = "Unable to initialize indexer";
            LOGGER.error(message, e);
            throw new TopologyGeneralException(message, e);
        }
    }

    private void indexRecord(StormTaskTuple stormTaskTuple, String useAltEnv, String database, IndexingProperties properties) throws IndexingException {
        final var indexerPool = indexerPoolWrapper.getIndexerPool(useAltEnv, database);

        final var document = new String(stormTaskTuple.getFileData());
        indexerPool.index(document, properties);
    }

    private void prepareTuple(StormTaskTuple stormTaskTuple, String useAltEnv, String datasetId,
                              String database, Date recordDate) {
        stormTaskTuple.setFileData((byte[]) null);
        var dataSetCleanerParameters = new DataSetCleanerParameters(datasetId,
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

    private void findAndUpdateHarvestedRecord(StormTaskTuple stormTaskTuple, String europeanaId) {
        String metisDatasetId = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
        Optional<HarvestedRecord> harvestedRecord = harvestedRecordsDAO.findRecord(metisDatasetId, europeanaId);

        if (harvestedRecord.isPresent()) {
            updateHarvestedRecord(harvestedRecord.get(), stormTaskTuple);
        } else {
            insertNewHarvestedRecord(metisDatasetId, europeanaId, stormTaskTuple);
        }
    }

    private void updateHarvestedRecord(HarvestedRecord harvestedRecord, StormTaskTuple stormTaskTuple) {
        Date latestHarvestDate = stormTaskTuple.isMarkedAsDeleted() ? null : harvestedRecord.getLatestHarvestDate();
        UUID latestHarvestMd5 = stormTaskTuple.isMarkedAsDeleted() ? null : harvestedRecord.getLatestHarvestMd5();

        if (isPublishEnvironment(stormTaskTuple)) {
            harvestedRecord.setPublishedHarvestDate(latestHarvestDate);
            harvestedRecord.setPublishedHarvestMd5(latestHarvestMd5);
        } else {
            harvestedRecord.setPreviewHarvestDate(latestHarvestDate);
            harvestedRecord.setPreviewHarvestMd5(latestHarvestMd5);
        }

        LOGGER.info("Updating harvested record for environment: {}, taskId: {}, recordId:{}, harvestedRecord: {}",
                stormTaskTuple.getParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE), harvestedRecord,
                stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl());
        harvestedRecordsDAO.insertHarvestedRecord(harvestedRecord);
    }

    private void insertNewHarvestedRecord(String metisDatasetId, String europeanaId,  StormTaskTuple stormTaskTuple) {
        var harvestedRecord = HarvestedRecord.builder().metisDatasetId(metisDatasetId)
                .recordLocalId(europeanaId).build();
        harvestedRecordsDAO.insertHarvestedRecord(harvestedRecord);
        LOGGER.warn("Could not find harvested record for europeanaId: {} and metisDatasetId: {}, Inserting new empty record! taskId: {}, recordId:{}",
                europeanaId, metisDatasetId, stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl());
    }

    private boolean isPublishEnvironment(StormTaskTuple stormTaskTuple) {
        final String database = stormTaskTuple.getParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE);
        switch (database) {
            case "PREVIEW":
                return false;
            case "PUBLISH":
                return true;
            default:
                throw new TopologyGeneralException("Unknown " + PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE + " : \"" + database + "\"");
        }

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
            var settingsGeneratorForDefaultEnv = new IndexingSettingsGenerator(indexingProperties);
            var settingsGeneratorForAnotherEnv = new IndexingSettingsGenerator(
                    TargetIndexingEnvironment.ALTERNATIVE, indexingProperties);

            var indexingSettingsForPreviewDbInDefaultEnv = settingsGeneratorForDefaultEnv.generateForPreview();
            var indexingSettingsForPublishDbInDefaultEnv = settingsGeneratorForDefaultEnv.generateForPublish();

            var indexingSettingsForPreviewDbInAnotherEnv = settingsGeneratorForAnotherEnv.generateForPreview();
            var indexingSettingsForPublishDbInAnotherEnv = settingsGeneratorForAnotherEnv.generateForPublish();

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
            throw new TopologyGeneralException("Specified environment and/or database is not recognized");
        }
    }
}
