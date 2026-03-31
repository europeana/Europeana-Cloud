package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.md5.FileMd5GenerationService;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.incremental.CategorizationParameters;
import eu.europeana.cloud.service.dps.storm.incremental.CategorizationResult;
import eu.europeana.cloud.service.dps.storm.service.HarvestedRecordCategorizationService;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HarvestedRecordCategorizationBolt extends AbstractDpsBolt {

  private static final Logger LOGGER = LoggerFactory.getLogger(HarvestedRecordCategorizationBolt.class);

  protected transient HarvestedRecordCategorizationService harvestedRecordCategorizationService;

  public HarvestedRecordCategorizationBolt(CassandraProperties cassandraProperties) {
    super(cassandraProperties);
  }

  @Override
  public void execute(Tuple anchorTuple, CommonTaskTuple t) {
    var categorizationParameters = prepareCategorizationParameters(t);
    LOGGER.info("Starting categorization for {}", categorizationParameters);
    //
    var categorizationResult = categorizeRecord(categorizationParameters);
    if (categorizationResult.shouldBeProcessed()) {
      LOGGER.info("Further processing will take place for {} and {}",
          categorizationResult.getCategorizationParameters(), categorizationResult.getHarvestedRecord());
      pushRecordToNextBolt(anchorTuple, t);
    } else {
      LOGGER.info("Further processing will be stopped for {} and {}",
          categorizationResult.getCategorizationParameters(), categorizationResult.getHarvestedRecord());
      ignoreRecordAsNotChanged(anchorTuple, t, categorizationResult);
    }
    outputCollector.ack(anchorTuple);
  }

  private CategorizationParameters prepareCategorizationParameters(CommonTaskTuple tuple) {
    return CategorizationParameters.builder()
            .fullHarvest(!isIncrementalHarvesting(tuple))
            .datasetId(tuple.getParameter(PluginParameterKeys.METIS_DATASET_ID))
            .europeanaId(tuple.getParameter(PluginParameterKeys.EUROPEANA_ID))
                                   .recordMd5(FileMd5GenerationService.generateUUID(tuple.getFileData()))
                                   .currentHarvestDate(DateHelper.parse(tuple.getParameter(PluginParameterKeys.HARVEST_DATE)))
                                   .recordDateStamp(tuple.getParameter(PluginParameterKeys.RECORD_DATESTAMP) != null ?
                                       DateHelper.parse(tuple.getParameter(PluginParameterKeys.RECORD_DATESTAMP))
                                       : null)
                                   .build();
  }

  private boolean isIncrementalHarvesting(CommonTaskTuple tuple) {
    return "true".equals(tuple.getParameter(PluginParameterKeys.INCREMENTAL_HARVEST));
  }

  private CategorizationResult categorizeRecord(CategorizationParameters categorizationParameters) {
    return harvestedRecordCategorizationService.categorize(categorizationParameters);
  }

  private void pushRecordToNextBolt(Tuple anchorTuple, CommonTaskTuple t) {
    outputCollector.emit(anchorTuple, t.toStormTuple());
  }

  private void ignoreRecordAsNotChanged(Tuple anchorTuple, CommonTaskTuple commonTaskTuple,
      CategorizationResult categorizationResult) {
    emitIgnoredNotification(
            anchorTuple,
            commonTaskTuple,
            "Record ignored.",
            "Record ignored in this incremental processing because it was already processed. Record datestamp: "
                    + categorizationResult.getCategorizationParameters().getRecordDateStamp()
                    + ".");
  }
}
