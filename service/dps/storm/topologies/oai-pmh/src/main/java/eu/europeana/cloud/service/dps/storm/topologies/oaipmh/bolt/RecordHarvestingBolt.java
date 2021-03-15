package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRepository;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.*;

/**
 * Storm bolt for harvesting single record from OAI endpoint.
 */
public class RecordHarvestingBolt extends AbstractDpsBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordHarvestingBolt.class);

    private transient OaiHarvester harvester;

    /**
     * For given: <br/>
     * <ul>
     * <li>OAI endpoint url</li>
     * <li>recordId</li>
     * <li>metadata prefix</li>
     * </ul>
     * <p>
     * record will be fetched from OAI endpoint. All need parameters should be provided in {@link StormTaskTuple}.
     *
     * @param stormTaskTuple
     */
    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        long harvestingStartTime = new Date().getTime();
        LOGGER.info("Starting harvesting for: {}", stormTaskTuple.getParameter(CLOUD_LOCAL_IDENTIFIER));
        String endpointLocation = readEndpointLocation(stormTaskTuple);
        String recordId = readRecordId(stormTaskTuple);
        String metadataPrefix = readMetadataPrefix(stormTaskTuple);
        if (parametersAreValid(endpointLocation, recordId, metadataPrefix)) {
            LOGGER.info("OAI Harvesting started for: {} and {}", recordId, endpointLocation);
            try (final InputStream record = harvester.harvestRecord(
                   new OaiRepository(endpointLocation, metadataPrefix), recordId)) {
                stormTaskTuple.setFileData(record);
                prepareLocalIdentifiers(stormTaskTuple);
                outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());

                LOGGER.info("Harvesting finished successfully for: {} and {}", recordId, endpointLocation);
            } catch (HarvesterException | IOException | EuropeanaIdException e) {
                LOGGER.error("Exception on harvesting", e);
                emitErrorNotification(
                        anchorTuple,
                        stormTaskTuple.getTaskId(),
                        stormTaskTuple.getFileUrl(),
                        "Error while harvesting a record",
                        "The full error is: " + e.getMessage() + ". The cause of the error is: " + e.getCause(),
                        StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
                LOGGER.error(e.getMessage());
            }
        } else {
            emitErrorNotification(
                    anchorTuple,
                    stormTaskTuple.getTaskId(),
                    stormTaskTuple.getParameter(DPS_TASK_INPUT_DATA),
                    "Invalid parameters",
                    null,
                    StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
        }
        LOGGER.info("Harvesting finished in: {}ms for {}", Calendar.getInstance().getTimeInMillis() - harvestingStartTime, stormTaskTuple.getParameter(CLOUD_LOCAL_IDENTIFIER));
        outputCollector.ack(anchorTuple);
    }

    @Override
    protected void cleanInvalidData(StormTaskTuple tuple) {
        int tries = tuple.getRecordAttemptNumber();
        LOGGER.info("Retry number {} detected. No cleaning phase required. Record will be harvested again.", tries);
    }

    private void prepareLocalIdentifiers(StormTaskTuple stormTaskTuple) throws EuropeanaIdException{
        String datasetId = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
        String document = new String(stormTaskTuple.getFileData());
        EuropeanaIdCreator europeanaIdCreator = new EuropeanaIdCreator();
        EuropeanaGeneratedIdsMap europeanaIdMap = europeanaIdCreator.constructEuropeanaId(document, datasetId);
        String europeanaId = europeanaIdMap.getEuropeanaGeneratedId();
        String localIdFromProvider = europeanaIdMap.getSourceProvidedChoAbout();
        //
        stormTaskTuple.getIdentifiersToUse().add(stormTaskTuple.getParameter(CLOUD_LOCAL_IDENTIFIER));
        stormTaskTuple.getIdentifiersToUse().add(europeanaId);
        stormTaskTuple.getIdentifiersToUse().add(localIdFromProvider);
    }

    @Override
    public void prepare() {
        harvester = HarvesterFactory.createOaiHarvester(null, DEFAULT_RETRIES, SLEEP_TIME);
    }

    private boolean parametersAreValid(String endpointLocation, String recordId, String metadataPrefix) {
        return endpointLocation != null && recordId != null && metadataPrefix != null;
    }

    private String readEndpointLocation(StormTaskTuple stormTaskTuple) {
        return stormTaskTuple.getParameter(DPS_TASK_INPUT_DATA);
    }

    private String readRecordId(StormTaskTuple stormTaskTuple) {
        return stormTaskTuple.getParameter(CLOUD_LOCAL_IDENTIFIER);
    }

    private String readMetadataPrefix(StormTaskTuple stormTaskTuple) {
        return stormTaskTuple.getParameter(PluginParameterKeys.SCHEMA_NAME);
    }

}
