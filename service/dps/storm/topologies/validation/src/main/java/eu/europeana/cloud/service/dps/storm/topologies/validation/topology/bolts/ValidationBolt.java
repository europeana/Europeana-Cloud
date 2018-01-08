package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.validation.model.ValidationResult;
import eu.europeana.validation.service.ValidationExecutionService;

import java.io.ByteArrayInputStream;

/**
 * Created by Tarek on 12/5/2017.
 */
public class ValidationBolt extends AbstractDpsBolt {
    private ValidationExecutionService validationService;

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        ValidationResult result = validateFile(stormTaskTuple);
        if (result.isSuccess()) {
            outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());
        }
        else {
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), result.getMessage(), null);
        }
    }

    private ValidationResult validateFile(StormTaskTuple stormTaskTuple) {
        String document = new String(stormTaskTuple.getFileData());
        return validationService.singleValidation(getSchemaName(stormTaskTuple), document);
    }

    @Override
    public void prepare() {
        validationService = new ValidationExecutionService();
    }

    private String getSchemaName(StormTaskTuple stormTaskTuple) {
        return stormTaskTuple.getParameter(PluginParameterKeys.SCHEMA_NAME);
    }
}
