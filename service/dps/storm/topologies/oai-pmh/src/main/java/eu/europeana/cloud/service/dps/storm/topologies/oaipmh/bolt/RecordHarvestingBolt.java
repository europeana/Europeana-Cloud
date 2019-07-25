package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester.Harvester;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.exceptions.HarvesterException;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.DPS_TASK_INPUT_DATA;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER;

/**
 * Storm bolt for harvesting single record from OAI endpoint.
 */
public class RecordHarvestingBolt extends AbstractDpsBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordHarvestingBolt.class);

    private static final String METADATA_XPATH = "/*[local-name()='OAI-PMH']" +
            "/*[local-name()='GetRecord']" +
            "/*[local-name()='record']" +
            "/*[local-name()='metadata']" +
            "/child::*";

    private static final String IS_DELETED_XPATH = "string(/*[local-name()='OAI-PMH']" +
            "/*[local-name()='GetRecord']" +
            "/*[local-name()='record']" +
            "/*[local-name()='header']" +
            "/@status)";

    private Harvester harvester;

    private XPathExpression expr;
    private XPathExpression isDeletedExpression;

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
    public void execute(StormTaskTuple stormTaskTuple) {
        String endpointLocation = readEndpointLocation(stormTaskTuple);
        String recordId = readRecordId(stormTaskTuple);
        String metadataPrefix = readMetadataPrefix(stormTaskTuple);
        if (parametersAreValid(endpointLocation, recordId, metadataPrefix)) {
            LOGGER.info("OAI Harvesting started for: {} and {}", recordId, endpointLocation);
            try (final InputStream record = harvester.harvestRecord(endpointLocation, recordId,
                    metadataPrefix, expr, isDeletedExpression)) {
                stormTaskTuple.setFileData(record);

                if (useHeaderIdentifier(stormTaskTuple))
                    trimLocalId(stormTaskTuple); //Added for the time of migration - MET-1189
                else
                    useEuropeanaId(stormTaskTuple);

                outputCollector.emit(stormTaskTuple.toStormTuple());
                LOGGER.info("Harvesting finished successfully for: {} and {}", recordId, endpointLocation);
            } catch (HarvesterException | IOException | EuropeanaIdException e) {
                LOGGER.error("Exception on harvesting", e);
                emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), "Error while harvesting a record", "The full error is: " + e.getMessage() + ". The cause of the error is: " + e.getCause());
                LOGGER.error(e.getMessage());
            }
        } else {
            emitErrorNotification(
                    stormTaskTuple.getTaskId(),
                    stormTaskTuple.getParameter(DPS_TASK_INPUT_DATA),
                    "Invalid parameters",
                    null);
        }
    }

    private void trimLocalId(StormTaskTuple stormTaskTuple) {
        String europeanaIdPrefix = stormTaskTuple.getParameter(PluginParameterKeys.MIGRATION_IDENTIFIER_PREFIX);
        String localId = stormTaskTuple.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER);
        if (europeanaIdPrefix != null && localId.startsWith(europeanaIdPrefix)) {
            String trimmed = localId.replace(europeanaIdPrefix, "");
            stormTaskTuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, trimmed);
        }
    }

    private void useEuropeanaId(StormTaskTuple stormTaskTuple) throws EuropeanaIdException {
        String datasetId = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
        String document = new String(stormTaskTuple.getFileData());
        EuropeanaIdCreator europeanaIdCreator = new EuropeanaIdCreator();
        EuropeanaGeneratedIdsMap europeanaIdMap = europeanaIdCreator.constructEuropeanaId(document, datasetId);
        String europeanaId = europeanaIdMap.getEuropeanaGeneratedId();
        String localIdFromProvider = europeanaIdMap.getSourceProvidedChoAbout();
        stormTaskTuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, europeanaId);
        stormTaskTuple.addParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER, localIdFromProvider);
    }


    @Override
    public void prepare() {

        harvester = new Harvester();

        try {
            XPath xpath = XPathFactory.newInstance().newXPath();
            expr = xpath.compile(METADATA_XPATH);
            isDeletedExpression = xpath.compile(IS_DELETED_XPATH);
        } catch (Exception e) {
            LOGGER.error("Exception while compiling the meta data xpath");
        }
    }

    private boolean parametersAreValid(String endpointLocation, String recordId, String metadataPrefix) {
        if (endpointLocation != null && recordId != null && metadataPrefix != null)
            return true;
        return false;
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

    private boolean useHeaderIdentifier(StormTaskTuple stormTaskTuple) {
        boolean useHeaderIdentifiers = false;
        if ("true".equals(stormTaskTuple.getParameter(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS))) {
            useHeaderIdentifiers = true;
        }
        return useHeaderIdentifiers;
    }
}
