package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.oaipmh.Harvester;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterException;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterFactory;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.DPS_TASK_INPUT_DATA;

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

    private transient Harvester harvester;

    private transient XPathExpression expr;
    private transient XPathExpression isDeletedExpression;

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
            try (final InputStream record = harvester.harvestRecord(endpointLocation, recordId,
                    metadataPrefix, expr, isDeletedExpression)) {
                stormTaskTuple.setFileData(record);

                if (useHeaderIdentifier(stormTaskTuple))
                    trimLocalId(stormTaskTuple); //Added for the time of migration - MET-1189
                else
                    useEuropeanaId(stormTaskTuple);

                outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
                outputCollector.ack(anchorTuple);

                LOGGER.info("Harvesting finished successfully for: {} and {}", recordId, endpointLocation);
            } catch (HarvesterException | IOException | EuropeanaIdException e) {
                LOGGER.error("Exception on harvesting", e);
                emitErrorNotification(
                        anchorTuple,
                        stormTaskTuple.getTaskId(),
                        stormTaskTuple.getFileUrl(),
                        "Error while harvesting a record",
                        "The full error is: " + e.getMessage() + ". The cause of the error is: " + e.getCause());
                LOGGER.error(e.getMessage());
            }
        } else {
            emitErrorNotification(
                    anchorTuple,
                    stormTaskTuple.getTaskId(),
                    stormTaskTuple.getParameter(DPS_TASK_INPUT_DATA),
                    "Invalid parameters",
                    null);
        }
        LOGGER.info("Harvesting finished in: {}ms for {}", Calendar.getInstance().getTimeInMillis() - harvestingStartTime, stormTaskTuple.getParameter(CLOUD_LOCAL_IDENTIFIER));
    }

    @Override
    protected void cleanInvalidData(StormTaskTuple tuple) {
        int tries = tuple.getRecordAttemptNumber();
        LOGGER.error("Retry number {} detected. No cleaning phase required. Record will be harvested again.", tries);
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
        harvester = HarvesterFactory.createHarvester(DEFAULT_RETRIES, SLEEP_TIME);

        try {
            XPath xpath = prepareXPathInstance();
            expr = xpath.compile(METADATA_XPATH);
            isDeletedExpression = xpath.compile(IS_DELETED_XPATH);
        } catch (Exception e) {
            LOGGER.error("Exception while compiling the meta data xpath");
        }
    }

    private XPath prepareXPathInstance() {
        /*
        We are using non-standard XPatch implementation by purpose.
        The standard one contains some static content that sometimes causes the threading issues.
        Exception that we encountered was:
            javax.xml.xpath.XPathExpressionException: org.xml.sax.SAXException: FWK005 parse may not be called while parsing.
         */
        return new net.sf.saxon.xpath.XPathFactoryImpl().newXPath();
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

    private boolean useHeaderIdentifier(StormTaskTuple stormTaskTuple) {
        boolean useHeaderIdentifiers = false;
        if ("true".equals(stormTaskTuple.getParameter(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS))) {
            useHeaderIdentifiers = true;
        }
        return useHeaderIdentifiers;
    }
}
