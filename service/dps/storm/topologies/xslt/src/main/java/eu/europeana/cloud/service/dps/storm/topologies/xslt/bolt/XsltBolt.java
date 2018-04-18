package eu.europeana.cloud.service.dps.storm.topologies.xslt.bolt;

import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.transformation.service.XsltTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;

public class XsltBolt extends AbstractDpsBolt {

    public static final Logger LOGGER = LoggerFactory.getLogger(XsltBolt.class);

    private XsltTransformer transformer;

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {

        StringWriter writer = null;
        try {
            String fileUrl = stormTaskTuple.getFileUrl();
            String xsltUrl = stormTaskTuple.getParameter(PluginParameterKeys.XSLT_URL);
            LOGGER.info("processing file: {} with xslt schema:{}", fileUrl, xsltUrl);
            String datasetValue = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
            writer = transformer.transform(xsltUrl, fileToTransform(stormTaskTuple), datasetValue);
            LOGGER.info("XsltBolt: transformation success for: {}", fileUrl);
            stormTaskTuple.setFileData(writer.toString().getBytes(Charset.forName("UTF-8")));

            final UrlParser urlParser = new UrlParser(fileUrl);
            if (urlParser.isUrlToRepresentationVersionFile()) {
                stormTaskTuple.addParameter(PluginParameterKeys.CLOUD_ID, urlParser.getPart(UrlPart.RECORDS));
                stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_NAME, urlParser.getPart(UrlPart.REPRESENTATIONS));
                stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_VERSION, urlParser.getPart(UrlPart.VERSIONS));
            }
            stormTaskTuple.getParameters().remove(PluginParameterKeys.XSLT_URL);

            outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());
        } catch (Exception e) {
            LOGGER.error("XsltBolt error:" + e.getMessage());
            emitErrorNotification(stormTaskTuple.getTaskId(), "", e.getMessage(), stormTaskTuple
                    .getParameters().toString());
        } finally {
            if (writer != null)
                try {
                    writer.close();
                } catch (IOException e) {
                    LOGGER.error("error: during closing the writter" + e.getMessage());
                }
        }
    }

    private byte[] fileToTransform(StormTaskTuple stormTaskTuple) {
        return stormTaskTuple.getFileData();
    }

    @Override
    public void prepare() {
        transformer = new XsltTransformer();
    }
}