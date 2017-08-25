package eu.europeana.cloud.service.dps.storm.xslt;

import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.util.LRUCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.Charset;

public class XsltBolt extends AbstractDpsBolt {

    public static final Logger LOGGER = LoggerFactory.getLogger(XsltBolt.class);

    private static LRUCache<String, Transformer> cache = new LRUCache<String, Transformer>(
            50);

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        InputStream stream = null;
        StringWriter writer = null;
        try {
            String fileUrl = stormTaskTuple.getFileUrl();
            byte[] fileContent = stormTaskTuple.getFileData();
            String xsltUrl = stormTaskTuple.getParameter(PluginParameterKeys.XSLT_URL);
            LOGGER.info("processing file: {} with xslt schema:{}", fileUrl, xsltUrl);
            Transformer transformer;
            if (cache.containsKey(xsltUrl)) {
                transformer = cache.get(xsltUrl);
            } else {
                Source xslDoc = new StreamSource(new URL(xsltUrl).openStream());
                TransformerFactory tFactory = TransformerFactory.newInstance();
                transformer = tFactory.newTransformer(xslDoc);
                cache.put(xsltUrl, transformer);
            }
            stream = new ByteArrayInputStream(fileContent);
            Source xmlDoc = new StreamSource(stream);
            writer = new StringWriter();
            transformer.transform(xmlDoc, new StreamResult(writer));
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

            if (writer != null)
                try {
                    writer.close();
                } catch (IOException e) {
                    LOGGER.error("error: during closing the writter" + e.getMessage());
                }
        }
    }

    @Override
    public void prepare() {
    }
}

