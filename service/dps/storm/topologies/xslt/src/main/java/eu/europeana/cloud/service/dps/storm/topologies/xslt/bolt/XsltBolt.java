package eu.europeana.cloud.service.dps.storm.topologies.xslt.bolt;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
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
  public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {

    StringWriter writer = null;
    try {
      final String fileUrl = stormTaskTuple.getFileUrl();
      final String xsltUrl = stormTaskTuple.getParameter(PluginParameterKeys.XSLT_URL);
      LOGGER.info("Processing file: {} with xslt schema:{}", fileUrl, xsltUrl);
      final XsltTransformer xsltTransformer = prepareXsltTransformer(stormTaskTuple);
      writer = xsltTransformer
          .transform(stormTaskTuple.getFileData(), prepareEuropeanaGeneratedIdsMap(stormTaskTuple));
      LOGGER.info("XsltBolt: transformation success for: {}", fileUrl);
      stormTaskTuple.setFileData(writer.toString().getBytes(StandardCharsets.UTF_8));

      final UrlParser urlParser = new UrlParser(fileUrl);
      if (urlParser.isUrlToRepresentationVersionFile()) {
        stormTaskTuple
            .addParameter(PluginParameterKeys.CLOUD_ID, urlParser.getPart(UrlPart.RECORDS));
        stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_NAME,
            urlParser.getPart(UrlPart.REPRESENTATIONS));
        stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_VERSION,
            urlParser.getPart(UrlPart.VERSIONS));
      }
      clearParametersStormTuple(stormTaskTuple);

      outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
      outputCollector.ack(anchorTuple);
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
    } catch (Exception e) {
      LOGGER.error("XsltBolt error:{}", e.getMessage());
        emitErrorNotification(anchorTuple, stormTaskTuple, e.getMessage(), ExceptionUtils.getStackTrace(e));
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

  private XsltTransformer prepareXsltTransformer(StormTaskTuple stormTaskTuple)
      throws TransformationException {
    //Get topology parameters
    final String xsltUrl = stormTaskTuple.getParameter(PluginParameterKeys.XSLT_URL);
    final String metisDatasetName = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_NAME);
    final String metisDatasetCountry = stormTaskTuple
        .getParameter(PluginParameterKeys.METIS_DATASET_COUNTRY);
    final String metisDatasetLanguage = stormTaskTuple
        .getParameter(PluginParameterKeys.METIS_DATASET_LANGUAGE);

    return new XsltTransformer(xsltUrl, metisDatasetName, metisDatasetCountry,
        metisDatasetLanguage);
  }

  private EuropeanaGeneratedIdsMap prepareEuropeanaGeneratedIdsMap(StormTaskTuple stormTaskTuple)
      throws EuropeanaIdException {
    String metisDatasetId = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
    //Prepare europeana identifiers
    EuropeanaGeneratedIdsMap europeanaGeneratedIdsMap = null;
    if (!StringUtils.isBlank(metisDatasetId)) {
      String fileDataString = new String(stormTaskTuple.getFileData(), StandardCharsets.UTF_8);
      EuropeanaIdCreator europeanIdCreator = new EuropeanaIdCreator();
      europeanaGeneratedIdsMap = europeanIdCreator
          .constructEuropeanaId(fileDataString, metisDatasetId);
    }
    return europeanaGeneratedIdsMap;
  }

  private void clearParametersStormTuple(StormTaskTuple stormTaskTuple) {
    stormTaskTuple.getParameters().remove(PluginParameterKeys.XSLT_URL);
    stormTaskTuple.getParameters().remove(PluginParameterKeys.METIS_DATASET_ID);
    stormTaskTuple.getParameters().remove(PluginParameterKeys.METIS_DATASET_NAME);
    stormTaskTuple.getParameters().remove(PluginParameterKeys.METIS_DATASET_COUNTRY);
    stormTaskTuple.getParameters().remove(PluginParameterKeys.METIS_DATASET_LANGUAGE);
  }

  @Override
  public void prepare() {
  }
}
