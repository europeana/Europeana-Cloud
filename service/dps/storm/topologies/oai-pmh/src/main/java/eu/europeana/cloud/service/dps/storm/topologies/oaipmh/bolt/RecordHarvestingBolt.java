package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import com.lyncode.xoai.model.oaipmh.Verb;
import com.lyncode.xoai.serviceprovider.client.OAIClient;
import com.lyncode.xoai.serviceprovider.exceptions.OAIRequestException;
import com.lyncode.xoai.serviceprovider.parameters.GetRecordParameters;
import com.lyncode.xoai.serviceprovider.parameters.Parameters;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helpers.OAIClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Storm bolt for harvesting single record from OAI endpoint.
 *
 */
public class RecordHarvestingBolt extends AbstractDpsBolt {

    public static final Logger LOGGER = LoggerFactory.getLogger(RecordHarvestingBolt.class);

    private OAIClientProvider oaiClientProvider;



    /**
     * For given: <br/>
     * <ul>
     * <li>OAI endpoint url</li>
     * <li>recordId</li>
     * <li>metadata prefix</li>
     * </ul>
     *
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
            try {
                LOGGER.info("OAI Harvesting started for: {} and {}", recordId, endpointLocation);

                InputStream harvestedRecord = harvestRecord(endpointLocation, recordId, metadataPrefix);
                stormTaskTuple.setFileData(harvestedRecord);
                outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());

                LOGGER.info("Harvesting finished successfully for: {} and {}", recordId, endpointLocation);
            } catch (OAIRequestException | IOException | NullPointerException e) {
                LOGGER.error(e.getMessage());
                StringWriter stack = new StringWriter();
                e.printStackTrace(new PrintWriter(stack));
                emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), "Cannot harvest data because: " + e.getMessage(),
                        stack.toString());

                LOGGER.error(e.getMessage());
            }
        } else {
            emitErrorNotification(
                    stormTaskTuple.getTaskId(),
                    stormTaskTuple.getParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA),
                    "Invalid parameters",
                    null);
        }
    }

    @Override
    public void prepare() {
        oaiClientProvider=new OAIClientProvider();
    }

    private boolean parametersAreValid(String endpointLocation, String recordId, String metadataPrefix) {
        if (endpointLocation != null && recordId != null && metadataPrefix != null)
            return true;
        else
            return false;
    }

    private String readEndpointLocation(StormTaskTuple stormTaskTuple) {
        return stormTaskTuple.getParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA);
    }

    private String readRecordId(StormTaskTuple stormTaskTuple) {
        return stormTaskTuple.getParameter(PluginParameterKeys.OAI_IDENTIFIER);
    }

    private String readMetadataPrefix(StormTaskTuple stormTaskTuple) {
        return stormTaskTuple.getSourceDetails().getSchema();
    }

    private InputStream harvestRecord(String endpointLocation, String recordId, String metadataPrefix) throws OAIRequestException {
        OAIClient client = prepareOAIClient(endpointLocation);
        GetRecordParameters params = prepareGetRecordParams(recordId, metadataPrefix);

        return client.execute(Parameters.parameters().withVerb(Verb.Type.GetRecord).include(params));
    }

    protected OAIClient prepareOAIClient(String endpointLocation) {
        return oaiClientProvider.provide(endpointLocation);
    }

    private GetRecordParameters prepareGetRecordParams(String recordId, String metadataPrefix) {
        return new GetRecordParameters().withIdentifier(recordId).withMetadataFormatPrefix(metadataPrefix);
    }

}
