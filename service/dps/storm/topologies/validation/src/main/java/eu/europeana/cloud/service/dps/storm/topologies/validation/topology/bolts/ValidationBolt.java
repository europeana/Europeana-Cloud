package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.BoltInitializationException;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.ValidationTopologyPropertiesKeys;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
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

public class ValidationBolt extends AbstractDpsBolt {

  public static final Logger LOGGER = LoggerFactory.getLogger(ValidationBolt.class);
  private static final long serialVersionUID = 1L;
  private final Properties properties;
  private transient ValidationExecutionService validationService;
  private transient XsltTransformer transformer;

  public ValidationBolt(CassandraProperties cassandraProperties, Properties properties) {
    super(cassandraProperties);
    this.properties = properties;
  }

  @Override
  public void execute(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
    try {
      reorderFileContent(commonTaskTuple);
      validateFileAndEmit(anchorTuple, commonTaskTuple);
      outputCollector.ack(anchorTuple);
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
    } catch (Exception e) {
      LOGGER.error("Validation Bolt error: {}", e.getMessage(), e);
      emitErrorNotification(anchorTuple, commonTaskTuple, e.getMessage(),
          "Error while validation. The full error :" + ExceptionUtils.getStackTrace(e));
      outputCollector.ack(anchorTuple);
    }
  }

  private void reorderFileContent(CommonTaskTuple commonTaskTuple) throws TransformationException {
    LOGGER.info("Reordering the file");
    StringWriter writer = transformer.transform(commonTaskTuple.getFileData(), null);
    commonTaskTuple.setFileData(writer.toString().getBytes(StandardCharsets.UTF_8));
  }

  private void validateFileAndEmit(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
    String document = new String(commonTaskTuple.getFileData(), StandardCharsets.UTF_8);
    ValidationResult result =
        validationService.singleValidation(
            getSchemaName(commonTaskTuple), getRootLocation(commonTaskTuple),
            getSchematronLocation(commonTaskTuple), document
        );
    if (result.isSuccess()) {
      outputCollector.emit(anchorTuple, commonTaskTuple.toStormTuple());
    } else {
      emitErrorNotification(anchorTuple, commonTaskTuple, result.getMessage(), getAdditionalInfo(result));
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
      throw new BoltInitializationException("Exception while initializing the transformer", ex);
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

  private String getSchemaName(CommonTaskTuple commonTaskTuple) {
    return commonTaskTuple.getParameter(PluginParameterKeys.SCHEMA_NAME);
  }

  private String getRootLocation(CommonTaskTuple commonTaskTuple) {
    return commonTaskTuple.getParameter(PluginParameterKeys.ROOT_LOCATION);
  }

  private String getSchematronLocation(CommonTaskTuple commonTaskTuple) {
    return commonTaskTuple.getParameter(PluginParameterKeys.SCHEMATRON_LOCATION);
  }
}
