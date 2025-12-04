package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexedRecordRemover;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.TopologyGeneralException;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.indexing.IndexingProperties;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.tiers.TierCalculationMode;
import eu.europeana.indexing.tiers.model.MediaTier;
import eu.europeana.metis.utils.DepublicationReason;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by pwozniak on 4/6/18
 */
public class IndexingBolt extends AbstractDpsBolt {

  public static final String PARSE_RECORD_DATE_ERROR_MESSAGE = "Could not parse RECORD_DATE parameter";
  public static final String INDEXING_FILE_ERROR_MESSAGE = "Unable to index file";
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexingBolt.class);
  private transient IndexWrapper indexWrapper;
  private final Properties indexingProperties;
  private transient HarvestedRecordsDAO harvestedRecordsDAO;
  private final String uisAddress;
  private final String topologyUserName;
  private final String topologyUserPassword;
  private transient EuropeanaIdFinder europeanaIdFinder;
  private transient IndexedRecordRemover recordRemover;


  public IndexingBolt(
      CassandraProperties cassandraProperties,
      Properties indexingProperties,
      String uisAddress, String topologyUserName,
      String topologyUserPassword) {
    super(cassandraProperties);
    this.indexingProperties = indexingProperties;
    this.uisAddress = uisAddress;
    this.topologyUserName = topologyUserName;
    this.topologyUserPassword = topologyUserPassword;
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
    recordRemover = new IndexedRecordRemover(indexWrapper);
  }

  @Override
  public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    // Get variables.
    final var database = getDatabase(stormTaskTuple);
    final var preserveTimestampsString = Boolean
        .parseBoolean(stormTaskTuple.getParameter(PluginParameterKeys.METIS_PRESERVE_TIMESTAMPS));
    final var datasetIdsToRedirectFrom = stormTaskTuple
            .getParameter(PluginParameterKeys.DATASET_IDS_TO_REDIRECT_FROM);
    final var datasetIdsToRedirectFromList = datasetIdsToRedirectFrom == null ? null
            : Arrays.stream(datasetIdsToRedirectFrom.split(",")).map(String::trim).toList();
    final var performRedirects = Boolean
            .parseBoolean(stormTaskTuple.getParameter(PluginParameterKeys.PERFORM_REDIRECTS));
    final Date recordDate;
    try {
      recordDate = DateHelper.parseISODate(stormTaskTuple.getParameter(PluginParameterKeys.METIS_RECORD_DATE));
      validateHarvestDate(stormTaskTuple);
      final var properties = new IndexingProperties(recordDate,
          preserveTimestampsString, datasetIdsToRedirectFromList, performRedirects, TierCalculationMode.OVERWRITE);
      String metisDatasetId = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
      String europeanaId = europeanaIdFinder.findForFileUrl(metisDatasetId, stormTaskTuple.getFileUrl());

      boolean recordNotSuitableForPublication = false;
      if (!stormTaskTuple.isMarkedAsDeleted()) {
        recordNotSuitableForPublication = !indexRecord(stormTaskTuple, database, properties);
      }
      boolean recordShouldBeDeleted = stormTaskTuple.isMarkedAsDeleted() || recordNotSuitableForPublication;

      if (recordShouldBeDeleted) {
        removeIndexedRecord(stormTaskTuple, database, europeanaId);
      } else{
        indexWrapper.getIndexer(database).removeTombstone(europeanaId);
      }
      updateHarvestedRecord(stormTaskTuple, europeanaId, recordShouldBeDeleted);
      if (recordNotSuitableForPublication) {
        String information = "Record deleted from database " + database + ", cause it was in media tier 0!" +
            " EuropeanaId: " + europeanaId;
        emitErrorNotification(anchorTuple, stormTaskTuple, "Record not suitable for publication", information);
        LOGGER.warn(information);
      } else {
        prepareTuple(stormTaskTuple, europeanaId);
        outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
        LOGGER.info(
            "Indexing bolt executed for: {} (record date: {}, preserve timestamps: {}).",
            database, recordDate, preserveTimestampsString);
      }
      outputCollector.ack(anchorTuple);
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
    } catch (DateTimeParseException e) {
      logAndEmitError(anchorTuple, e, PARSE_RECORD_DATE_ERROR_MESSAGE, stormTaskTuple);
      outputCollector.ack(anchorTuple);
    } catch (RuntimeException | MalformedURLException | CloudException e) {
      logAndEmitError(anchorTuple, e, e.getMessage(), stormTaskTuple);
      outputCollector.ack(anchorTuple);
    } catch (IndexingException e) {
      logAndEmitError(anchorTuple, e, INDEXING_FILE_ERROR_MESSAGE, stormTaskTuple);
      outputCollector.ack(anchorTuple);
    }
  }

  private void removeIndexedRecord(StormTaskTuple stormTaskTuple, TargetIndexingDatabase database, String europeanaId)
      throws IndexingException {
    boolean tombstoneIsCreated = recordRemover.removeRecord(database, europeanaId, DepublicationReason.REMOVED_DATA_AT_SOURCE);
    LOGGER.info("Finished removing indexed record: "
            + "europeanaId: {}, database: {}, taskId: {}, recordId: {}, tombstone is created: {}",
        europeanaId, database, stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), tombstoneIsCreated);
  }

  private void prepareDao() {
    var cassandraConnectionProvider =
        CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
            cassandraProperties.getHosts(),
            cassandraProperties.getPort(),
            cassandraProperties.getKeyspace(),
            cassandraProperties.getUser(),
            cassandraProperties.getPassword());
    harvestedRecordsDAO = HarvestedRecordsDAO.getInstance(cassandraConnectionProvider);
  }

  private void prepareEuropeanaIdFinder() {
    europeanaIdFinder = new EuropeanaIdFinder(
        new UISClient(uisAddress, topologyUserName, topologyUserPassword),
        harvestedRecordsDAO);
  }

  private void prepareIndexer() {
    indexWrapper = IndexWrapper.getInstance(indexingProperties);
  }

  private boolean indexRecord(StormTaskTuple stormTaskTuple, TargetIndexingDatabase database,
      IndexingProperties properties) throws IndexingException {
    AtomicBoolean suitableForPublication = new AtomicBoolean();
    final var document = new String(stormTaskTuple.getFileData(), StandardCharsets.UTF_8);
    indexWrapper.getIndexer(database).index(document, properties, tier -> {
      suitableForPublication.set((database == TargetIndexingDatabase.PREVIEW) || (tier.getMediaTier() != MediaTier.T0));
      return suitableForPublication.get();
    });
    return suitableForPublication.get();
  }

  private void prepareTuple(StormTaskTuple stormTaskTuple, String europeanaId) {
    stormTaskTuple.setFileData((byte[]) null);
    stormTaskTuple.addParameter(PluginParameterKeys.EUROPEANA_ID, europeanaId);
  }

  private void logAndEmitError(Tuple anchorTuple, Exception e, String errorMessage, StormTaskTuple stormTaskTuple) {
    LOGGER.error(errorMessage, e);
    emitErrorNotification(anchorTuple, stormTaskTuple, errorMessage,
            "Error while indexing. The full error is: " + ExceptionUtils.getStackTrace(e));
  }

  private void updateHarvestedRecord(StormTaskTuple stormTaskTuple, String europeanaId, boolean recordDeleted) {
    String metisDatasetId = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);

    var harvestedRecord = harvestedRecordsDAO.findRecord(metisDatasetId, europeanaId)
                                             .orElseGet(
                                                 () -> prepareNewHarvestedRecord(stormTaskTuple, europeanaId, metisDatasetId));

    Date latestHarvestDate = recordDeleted ? null : harvestedRecord.getLatestHarvestDate();
    UUID latestHarvestMd5 = recordDeleted ? null : harvestedRecord.getLatestHarvestMd5();

    var database = getDatabase(stormTaskTuple);
    switch (database) {
      case PREVIEW:
        harvestedRecord.setPreviewHarvestDate(latestHarvestDate);
        harvestedRecord.setPreviewHarvestMd5(latestHarvestMd5);
        break;
      case PUBLISH:
        harvestedRecord.setPublishedHarvestDate(latestHarvestDate);
        harvestedRecord.setPublishedHarvestMd5(latestHarvestMd5);
        break;
      default:
        throw new TopologyGeneralException(
            "Unknown " + PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE + " : \"" + database + "\"");
    }

    LOGGER.info("Saving harvested record for environment: {}, taskId: {}, recordId:{}, harvestedRecord: {}",
        database, harvestedRecord, stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl());
    harvestedRecordsDAO.insertHarvestedRecord(harvestedRecord);
  }

  private HarvestedRecord prepareNewHarvestedRecord(StormTaskTuple stormTaskTuple, String europeanaId, String metisDatasetId) {
    LOGGER.warn(
        "Could not find harvested record for europeanaId: {} and metisDatasetId: {}, Creating new one! taskId: {}, recordId:{}",
        europeanaId, metisDatasetId, stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl());
    return HarvestedRecord.builder().metisDatasetId(metisDatasetId).recordLocalId(europeanaId)
                          .latestHarvestDate(stormTaskTuple.getHarvestDate()).build();
  }

  private TargetIndexingDatabase getDatabase(StormTaskTuple stormTaskTuple) {
    return TargetIndexingDatabase.valueOf(
        stormTaskTuple.getParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE));
  }

  private void validateHarvestDate(StormTaskTuple stormTaskTuple) throws DateTimeParseException {
    //throws DateTimeParseException if date could not be parsed
    stormTaskTuple.getHarvestDate();
  }

}
