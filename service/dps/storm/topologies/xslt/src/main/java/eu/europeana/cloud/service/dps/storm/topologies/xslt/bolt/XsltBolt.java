package eu.europeana.cloud.service.dps.storm.topologies.xslt.bolt;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.metis.transformation.service.*;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

public class XsltBolt extends AbstractDpsBolt {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(XsltBolt.class);

  public XsltBolt(CassandraProperties cassandraProperties) {
    super(cassandraProperties);
  }


  @Override
  public void execute(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {

    StringWriter writer = null;
    try {
      final String fileUrl = commonTaskTuple.getRecordUri();
        final String xsltUrl = commonTaskTuple.getParameter(PluginParameterKeys.XSLT_URL);
      LOGGER.info("Processing file: {} with xslt schema:{}", fileUrl, xsltUrl);
      final XsltTransformer xsltTransformer = prepareXsltTransformer(commonTaskTuple);
      writer = xsltTransformer
          .transform(commonTaskTuple.getFileData(), prepareEuropeanaGeneratedIdsMap(commonTaskTuple));
      LOGGER.info("XsltBolt: transformation success for: {}", fileUrl);
      commonTaskTuple.setFileData(writer.toString().getBytes(StandardCharsets.UTF_8));

      final UrlParser urlParser = new UrlParser(fileUrl);
      if (urlParser.isUrlToRepresentationVersionFile()) {
        commonTaskTuple
            .addParameter(PluginParameterKeys.CLOUD_ID, urlParser.getPart(UrlPart.RECORDS));
        commonTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_VERSION,
            urlParser.getPart(UrlPart.VERSIONS));
      }
      clearParametersStormTuple(commonTaskTuple);

      outputCollector.emit(anchorTuple, commonTaskTuple.toStormTuple());
      outputCollector.ack(anchorTuple);
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
    } catch (Exception e) {
      LOGGER.error("XsltBolt error:{}", e.getMessage());
        emitErrorNotification(anchorTuple, commonTaskTuple, e.getMessage(), ExceptionUtils.getStackTrace(e));
        outputCollector.ack(anchorTuple);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          LOGGER.error("Error: during closing the writer {}", e.getMessage());
        }
      }
    }
  }

  private XsltTransformer prepareXsltTransformer(CommonTaskTuple commonTaskTuple)
      throws TransformationException {
    //Get topology parameters
    final String xsltUrl = commonTaskTuple.getParameter(PluginParameterKeys.XSLT_URL);
    final String metisDatasetName = commonTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_NAME);
    final String metisDatasetCountry = commonTaskTuple
        .getParameter(PluginParameterKeys.METIS_DATASET_COUNTRY);
    final String metisDatasetLanguage = commonTaskTuple
        .getParameter(PluginParameterKeys.METIS_DATASET_LANGUAGE);

    return new XsltTransformer(xsltUrl, metisDatasetName, metisDatasetCountry,
        metisDatasetLanguage);
  }

  private EuropeanaGeneratedIdsMap prepareEuropeanaGeneratedIdsMap(CommonTaskTuple commonTaskTuple)
      throws EuropeanaIdException {
    String metisDatasetId = commonTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
    //Prepare europeana identifiers
    EuropeanaGeneratedIdsMap europeanaGeneratedIdsMap = null;
    if (!StringUtils.isBlank(metisDatasetId)) {
      String fileDataString = new String(commonTaskTuple.getFileData(), StandardCharsets.UTF_8);
      EuropeanaIdCreator europeanIdCreator = new EuropeanaIdCreator();
      europeanaGeneratedIdsMap = europeanIdCreator
          .constructEuropeanaId(fileDataString, metisDatasetId);
    }
    return europeanaGeneratedIdsMap;
  }

  private void clearParametersStormTuple(CommonTaskTuple commonTaskTuple) {
    commonTaskTuple.getParameters().remove(PluginParameterKeys.XSLT_URL);
    commonTaskTuple.getParameters().remove(PluginParameterKeys.METIS_DATASET_ID);
    commonTaskTuple.getParameters().remove(PluginParameterKeys.METIS_DATASET_NAME);
    commonTaskTuple.getParameters().remove(PluginParameterKeys.METIS_DATASET_COUNTRY);
    commonTaskTuple.getParameters().remove(PluginParameterKeys.METIS_DATASET_LANGUAGE);
  }

  @Override
  public void prepare() {
  }
}
