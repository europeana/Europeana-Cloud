package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.ValidationTopologyPropertiesKeys;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import eu.europeana.metis.transformation.service.TransformationException;
import eu.europeana.metis.transformation.service.XsltTransformer;
import eu.europeana.validation.model.ValidationResult;
import eu.europeana.validation.service.ValidationExecutionService;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Created by Tarek on 12/5/2017.
 */
public class ValidationBolt extends AbstractDpsBolt {
    public static final Logger LOGGER = LoggerFactory.getLogger(ValidationBolt.class);
    private static final long serialVersionUID = 1L;
    private final Properties properties;
    private transient ValidationExecutionService validationService;
    private transient XsltTransformer transformer;

    public ValidationBolt(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        try {
            reorderFileContent(stormTaskTuple);
            validateFileAndEmit(anchorTuple, stormTaskTuple);
        } catch (Exception e) {
            LOGGER.error("Validation Bolt error: {}", e.getMessage());
            emitErrorNotification(anchorTuple, stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Error while validation. The full error :" + ExceptionUtils.getStackTrace(e),
                    StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
        }
        outputCollector.ack(anchorTuple);
    }

    private void reorderFileContent(StormTaskTuple stormTaskTuple) throws TransformationException {
        LOGGER.info("Reordering the file");
        StringWriter writer = transformer.transform(stormTaskTuple.getFileData(), null);
        stormTaskTuple.setFileData(writer.toString().getBytes(StandardCharsets.UTF_8));
    }

    private void validateFileAndEmit(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        String document = new String(stormTaskTuple.getFileData());
        ValidationResult result = validationService.singleValidation(getSchemaName(stormTaskTuple), getRootLocation(stormTaskTuple), getSchematronLocation(stormTaskTuple), document);
        if (result.isSuccess()) {
            outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
        } else {
            emitErrorNotification(anchorTuple, stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), result.getMessage(), getAdditionalInfo(result),
                    StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
        }
    }

    @Override
    public void prepare() {
        validationService = new ValidationExecutionService(properties);
        try {
            String sorterFileLocation = properties.get(ValidationTopologyPropertiesKeys.EDM_SORTER_FILE_LOCATION).toString();
            LOGGER.info("Preparing XsltTransformer for {}", sorterFileLocation);
            transformer = new XsltTransformer(sorterFileLocation);
        } catch (TransformationException ex) {
            LOGGER.info("Exception while initializing the transformer");
        }
    }

    private String getAdditionalInfo(ValidationResult vr) {
        StringBuilder sb = new StringBuilder();
        if (vr.getRecordId() != null) {
            sb.append("recordId: ");
            sb.append(vr.getRecordId());
            sb.append(' ');
        }
        if (vr.getNodeId() != null) {
            sb.append("nodeId: ");
            sb.append(vr.getNodeId());
        }
        String additionalInfo = sb.toString();

        return !additionalInfo.isEmpty() ? additionalInfo : null;
    }

    private String getSchemaName(StormTaskTuple stormTaskTuple) {
        return stormTaskTuple.getParameter(PluginParameterKeys.SCHEMA_NAME);
    }

    private String getRootLocation(StormTaskTuple stormTaskTuple) {
        return stormTaskTuple.getParameter(PluginParameterKeys.ROOT_LOCATION);
    }

    private String getSchematronLocation(StormTaskTuple stormTaskTuple) {
        return stormTaskTuple.getParameter(PluginParameterKeys.SCHEMATRON_LOCATION);
    }
}
