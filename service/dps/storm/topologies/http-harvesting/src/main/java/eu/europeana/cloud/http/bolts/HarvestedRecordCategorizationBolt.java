package eu.europeana.cloud.http.bolts;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.service.commons.md5.FileMd5GenerationService;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.incremental.CategorizationParameters;
import eu.europeana.cloud.service.dps.storm.incremental.CategorizationResult;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.storm.utils.DbConnectionDetails;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        //
        var categorizationResult = categorizeRecord(categorizationParameters);
        if (categorizationResult.shouldBeProcessed()) {
            LOGGER.info("Further processing will take place for {} and {}", categorizationResult.getCategorizationParameters(), categorizationResult.getHarvestedRecord());
            pushRecordToNextBolt(anchorTuple, t);
        } else {
            LOGGER.info("Further processing will be stopped for {} and {}", categorizationResult.getCategorizationParameters(), categorizationResult.getHarvestedRecord());
            ignoreRecordAsNotChanged(anchorTuple, t, categorizationResult);
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
                .fullHarvest(!isIncrementalHarvesting(tuple))
                .datasetId(tuple.getParameter(PluginParameterKeys.METIS_DATASET_ID))
                .recordId(tuple.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER))
                .recordMd5(FileMd5GenerationService.generateUUID(tuple.getFileData()))
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
