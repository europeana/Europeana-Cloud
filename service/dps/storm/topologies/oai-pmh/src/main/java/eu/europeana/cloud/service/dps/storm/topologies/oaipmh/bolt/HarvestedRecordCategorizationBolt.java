package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.utils.CategorizationParameters;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.utils.CategorizationResult;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.utils.HarvestedRecordCategorizationService;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.storm.utils.DbConnectionDetails;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bolt that will categorize record in the incremental harvesting if it should be processed or not
 */
public class HarvestedRecordCategorizationBolt extends AbstractDpsBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(HarvestedRecordCategorizationBolt.class);

    private final DbConnectionDetails dbConnectionDetails;
    private transient HarvestedRecordCategorizationService harvestedRecordCategorizationService;

    public HarvestedRecordCategorizationBolt(DbConnectionDetails dbConnectionDetails) {
        this.dbConnectionDetails = dbConnectionDetails;
    }

    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple t) {
        var categorizationParameters = prepareCategorizationParameters(t);
        LOGGER.info("Starting categorization for {}", categorizationParameters);
        if (isIncrementalHarvesting(t)) {
            var categorizationResult = categorizeRecord(categorizationParameters);
            if (categorizationResult.shouldBeProcessed()) {
                LOGGER.info("Further processing will take place for {} and {}", categorizationResult.getCategorizationParameters(), categorizationResult.getHarvestedRecord());
                pushRecordToNextBolt(anchorTuple, t);
            } else {
                LOGGER.info("Further processing will be stopped for {} and {}", categorizationResult.getCategorizationParameters(), categorizationResult.getHarvestedRecord());
                ignoreRecordAsNotChanged(anchorTuple, t, categorizationResult);
            }
        } else {
            LOGGER.info("Categorization process will not be started for {}. It is not incremental processing", categorizationParameters);
            pushRecordToNextBolt(anchorTuple, t);
        }
        outputCollector.ack(anchorTuple);
    }

    @Override
    public void prepare() {
        var cassandraConnectionProvider =
                CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
                        dbConnectionDetails.getHosts(),
                        dbConnectionDetails.getPort(),
                        dbConnectionDetails.getKeyspaceName(),
                        dbConnectionDetails.getUserName(),
                        dbConnectionDetails.getPassword());

        harvestedRecordCategorizationService = new HarvestedRecordCategorizationService(new HarvestedRecordsDAO(cassandraConnectionProvider));
    }

    private CategorizationParameters prepareCategorizationParameters(StormTaskTuple tuple){

        return CategorizationParameters.builder()
                .datasetId(tuple.getParameter(PluginParameterKeys.METIS_DATASET_ID))
                .recordId(tuple.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER))
                .recordDateStamp(DateHelper.parse(tuple.getParameter(PluginParameterKeys.RECORD_DATESTAMP)))
                .currentHarvestDate(DateHelper.parse(tuple.getParameter(PluginParameterKeys.HARVEST_DATE)))
                .build();
    }

    private boolean isIncrementalHarvesting(StormTaskTuple tuple) {
        return "true".equals(tuple.getParameter(PluginParameterKeys.INCREMENTAL_HARVEST));
    }

    private CategorizationResult categorizeRecord(CategorizationParameters categorizationParameters) {
        return harvestedRecordCategorizationService.categorize(categorizationParameters);
    }

    private void pushRecordToNextBolt(Tuple anchorTuple, StormTaskTuple t) {
        outputCollector.emit(anchorTuple, t.toStormTuple());
    }

    private void ignoreRecordAsNotChanged(Tuple anchorTuple, StormTaskTuple stormTaskTuple, CategorizationResult categorizationResult) {
        emitSuccessNotification(
                anchorTuple,
                stormTaskTuple.getTaskId(),
                stormTaskTuple.getFileUrl(),
                "Record ignored.",
                "Record ignored in this incremental processing because it was already processed. Record datestamp: " + categorizationResult.getCategorizationParameters().getRecordDateStamp() + ".",
                "",
                StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
    }
}