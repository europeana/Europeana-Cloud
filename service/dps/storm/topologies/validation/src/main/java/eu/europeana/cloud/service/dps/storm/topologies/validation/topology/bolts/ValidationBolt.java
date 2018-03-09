package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.validation.model.ValidationResult;
import eu.europeana.validation.service.ValidationExecutionService;

import java.util.Properties;

/**
 * Created by Tarek on 12/5/2017.
 */
public class ValidationBolt extends AbstractDpsBolt {
    private ValidationExecutionService validationService;
    private Properties properties;

    public ValidationBolt(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        ValidationResult result = validateFile(stormTaskTuple);
        if (result.isSuccess()) {
            outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());
        } else {
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), result.getMessage(), getAdditionalInfo(result));
        }
    }

    private ValidationResult validateFile(StormTaskTuple stormTaskTuple) {
        String document = new String(stormTaskTuple.getFileData());
        return validationService.singleValidation(getSchemaName(stormTaskTuple), getRootLocation(stormTaskTuple), getSchematromLocation(stormTaskTuple), document);
    }

    @Override
    public void prepare() {
        validationService = new ValidationExecutionService(properties);
    }

    private String getAdditionalInfo(ValidationResult vr) {
        String additionalInfo = null;
        StringBuilder sb = new StringBuilder();
        if (vr.getRecordId() != null) {
            sb.append("recordId: ");
            sb.append(vr.getRecordId());
            sb.append(" ");
        }
        if (vr.getNodeId() != null) {
            sb.append("nodeId: ");
            sb.append(vr.getNodeId());
        }
        additionalInfo = sb.toString();

        return !additionalInfo.isEmpty() ? additionalInfo : null;
    }

    private String getSchemaName(StormTaskTuple stormTaskTuple) {
        return stormTaskTuple.getParameter(PluginParameterKeys.SCHEMA_NAME);
    }

    private String getRootLocation(StormTaskTuple stormTaskTuple) {
        return stormTaskTuple.getParameter(PluginParameterKeys.ROOT_LOCATION);
    }

    private String getSchematromLocation(StormTaskTuple stormTaskTuple) {
        return stormTaskTuple.getParameter(PluginParameterKeys.SCHEMATRON_LOCATION);
    }

}

