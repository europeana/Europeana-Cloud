package eu.europeana.cloud.service.dps.storm.topologies.xslt.bolt;

import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import eu.europeana.metis.transformation.service.TransformationException;
import eu.europeana.metis.transformation.service.XsltTransformer;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XsltBolt extends AbstractDpsBolt {

  private static final Logger LOGGER = LoggerFactory.getLogger(XsltBolt.class);


  @Override
  public void execute(StormTaskTuple stormTaskTuple) {

    StringWriter writer = null;
    try {
      String fileUrl = stormTaskTuple.getFileUrl();
      String xsltUrl = stormTaskTuple.getParameter(PluginParameterKeys.XSLT_URL);
      LOGGER.info("Processing file: {} with xslt schema:{}", fileUrl, xsltUrl);
      XsltTransformer xsltTransformer = prepareXsltTransformer(stormTaskTuple);
      writer = xsltTransformer
          .transform(stormTaskTuple.getFileData(), prepareEuropeanaGeneratedIdsMap(stormTaskTuple));
      LOGGER.info("XsltBolt: transformation success for: {}", fileUrl);
      stormTaskTuple.setFileData(writer.toString().getBytes(Charset.forName("UTF-8")));

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

      outputCollector.emit(stormTaskTuple.toStormTuple());
    } catch (Exception e) {
      LOGGER.error("XsltBolt error:{}",  e.getMessage());
      emitErrorNotification(stormTaskTuple.getTaskId(), "", e.getMessage(), stormTaskTuple
          .getParameters().toString());
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
    String xsltUrl = stormTaskTuple.getParameter(PluginParameterKeys.XSLT_URL);
    String metisDatasetName = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_NAME);
    String metisDatasetCountry = stormTaskTuple
        .getParameter(PluginParameterKeys.METIS_DATASET_COUNTRY);
    String metisDatasetLanguage = stormTaskTuple
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
      String fileDataString = new String(stormTaskTuple.getFileData());
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