package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import com.google.gson.Gson;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexingSettingsGenerator;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingEnvironment;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.IndexingSettings;
import eu.europeana.indexing.exception.IndexingException;

import java.io.Closeable;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

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

    private Properties indexingProperties;


    public IndexingBolt(Properties indexingProperties) {
        this.indexingProperties = indexingProperties;
    }

    @Override
    protected boolean ignoreDeleted() {
        return false;
    }

    @Override
    public void prepare() {
        try {
            indexerPoolWrapper = new IndexerPoolWrapper(MAX_IDLE_TIME_FOR_INDEXER_IN_SECS,
                    IDLE_TIME_CHECK_INTERVAL_IN_SECS);
        } catch (IndexingException | URISyntaxException e) {
            LOGGER.error("Unable to initialize indexer", e);
            throw new RuntimeException(e);
        }
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
