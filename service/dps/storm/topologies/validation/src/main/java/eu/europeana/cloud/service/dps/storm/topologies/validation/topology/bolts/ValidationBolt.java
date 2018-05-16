package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.transformation.service.TransformationException;
import eu.europeana.metis.transformation.service.XsltTransformer;
import eu.europeana.validation.model.ValidationResult;
import eu.europeana.validation.service.ValidationExecutionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Properties;

/**
 * Created by Tarek on 12/5/2017.
 */
public class ValidationBolt extends AbstractDpsBolt {

    public static final Logger LOGGER = LoggerFactory.getLogger(ValidationBolt.class);
    private static final String XSLT_SORTER_FILE_NAME = "edm_sorter.xsl";

    private ValidationExecutionService validationService;
    private XsltTransformer transformer;
    private Properties properties;

    public ValidationBolt(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        try {
            reorderFileContent(stormTaskTuple);
            validateFile(stormTaskTuple);
        } catch (Exception e) {
            LOGGER.error("XsltBolt error: {}", e.getMessage());
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), stormTaskTuple
                    .getParameters().toString());
        }
    }

    private void reorderFileContent(StormTaskTuple stormTaskTuple) throws TransformationException {
        LOGGER.info("Reordering the file");
        URL url = getClass().getClassLoader().getResource(XSLT_SORTER_FILE_NAME);
        StringWriter writer = transformer.transform(url.toString(), stormTaskTuple.getFileData());
        stormTaskTuple.setFileData(writer.toString().getBytes(Charset.forName("UTF-8")));
    }

    private void validateFile(StormTaskTuple stormTaskTuple) {
        String document = new String(stormTaskTuple.getFileData());
        ValidationResult result = validationService.singleValidation(getSchemaName(stormTaskTuple), getRootLocation(stormTaskTuple), getSchematromLocation(stormTaskTuple), document);
        if (result.isSuccess()) {
            outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());
        } else {
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), result.getMessage(), getAdditionalInfo(result));
        }
    }

    @Override
    public void prepare() {
        validationService = new ValidationExecutionService(properties);
        transformer = new XsltTransformer();
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
