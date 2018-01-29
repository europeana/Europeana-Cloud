package eu.europeana.cloud.service.dps.storm.xslt;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import eu.europeana.cloud.service.commons.urls.UrlPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.util.LRUCache;
import eu.europeana.cloud.service.commons.urls.UrlParser;

public class XsltBolt extends AbstractDpsBolt {

    public static final Logger LOGGER = LoggerFactory.getLogger(XsltBolt.class);

    private static LRUCache<String, Transformer> cache = new LRUCache<String, Transformer>(
            50);

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        InputStream stream = null;
        StringWriter writer = null;
        InputStream xsltStream = null;
        try {
            String fileUrl = stormTaskTuple.getFileUrl();
            byte[] fileContent = stormTaskTuple.getFileData();
            String xsltUrl = stormTaskTuple.getParameter(PluginParameterKeys.XSLT_URL);
            LOGGER.info("processing file: {} with xslt schema:{}", fileUrl, xsltUrl);
            Transformer transformer;

            if (cache.containsKey(xsltUrl)) {
                transformer = cache.get(xsltUrl);
            } else {
                xsltStream = new URL(xsltUrl).openStream();
                Source xslDoc = new StreamSource(xsltStream);
                TransformerFactory tFactory = TransformerFactory.newInstance();
                transformer = tFactory.newTransformer(xslDoc);
                cache.put(xsltUrl, transformer);
            }
            stream = new ByteArrayInputStream(fileContent);
            Source xmlDoc = new StreamSource(stream);
            writer = new StringWriter();
            transformer.transform(xmlDoc, new StreamResult(writer));
            // metis specific
            injectMetisDatasetId(stormTaskTuple, transformer);

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
        } catch (TransformerConfigurationException e) {
            LOGGER.error("XsltBolt error:" + e.getMessage());
            emitDropNotification(stormTaskTuple.getTaskId(), "", e.getMessage(), stormTaskTuple
                    .getParameters().toString());
        } catch (TransformerException e) {
            LOGGER.error("XsltBolt error:" + e.getMessage());
            emitDropNotification(stormTaskTuple.getTaskId(), "", e.getMessage(), stormTaskTuple
                    .getParameters().toString());
        } catch (Exception e) {
            LOGGER.error("XsltBolt error:" + e.getMessage());
            emitDropNotification(stormTaskTuple.getTaskId(), "", e.getMessage(), stormTaskTuple
                    .getParameters().toString());
        } finally {
            if (stream != null)
                try {
                    stream.close();
                } catch (IOException e) {
                    LOGGER.error("error: during closing the stream" + e.getMessage());
                }
            if (xsltStream != null)
                try {
                    xsltStream.close();
                } catch (IOException e) {
                    LOGGER.error("error: during closing the stream" + e.getMessage());
                }

            if (writer != null)
                try {
                    writer.close();
                } catch (IOException e) {
                    LOGGER.error("error: during closing the writter" + e.getMessage());
                }
        }
    }

    private void injectMetisDatasetId(StormTaskTuple stormTaskTuple, Transformer transformer) {
        String value = stormTaskTuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
        if (value != null || !value.isEmpty()) {
            transformer.setParameter(PluginParameterKeys.METIS_DATASET_ID_PARAMETER_NAME, value);
        }
    }

    @Override
    public void prepare() {
    }
}

