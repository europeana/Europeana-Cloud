package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester.Harvester;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.exceptions.HarvesterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.DPS_TASK_INPUT_DATA;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER;

/**
 * Storm bolt for harvesting single record from OAI endpoint.
 */
public class RecordHarvestingBolt extends AbstractDpsBolt {
    private Harvester harvester;
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordHarvestingBolt.class);
    private static final String METADATA_XPATH = "/*[local-name()='OAI-PMH']" +
            "/*[local-name()='GetRecord']" +
            "/*[local-name()='record']" +
            "/*[local-name()='metadata']" +
            "/child::*";
    private XPathExpression expr;


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
                    metadataPrefix, expr)) {
                stormTaskTuple.setFileData(record);
                outputCollector.emit(stormTaskTuple.toStormTuple());
                LOGGER.info("Harvesting finished successfully for: {} and {}", recordId, endpointLocation);
            } catch (HarvesterException | IOException e) {
                LOGGER.error("Exception on harvesting", e);
                StringWriter stack = new StringWriter();
                e.printStackTrace(new PrintWriter(stack));
                emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), "Cannot harvest data because: " + e.getMessage(),
                        stack.toString());

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

    @Override
    public void prepare() {
        harvester = new Harvester();
        try {
            XPath xpath = XPathFactory.newInstance().newXPath();
            expr = xpath.compile(METADATA_XPATH);
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


}
