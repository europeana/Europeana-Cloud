package eu.europeana.cloud.service.dps.storm.xslt;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.Charset;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.util.LRUCache;

public class XsltBolt extends AbstractDpsBolt {

    public static final Logger LOGGER = LoggerFactory.getLogger(XsltBolt.class);

    private LRUCache<String, Transformer> cache = new LRUCache<String, Transformer>(
            50);

    @Override
    public void execute(StormTaskTuple t) {

        try {
            String fileUrl = t.getFileUrl();
            byte[] fileContent = t.getFileData();
            String xsltUrl = t.getParameter(PluginParameterKeys.XSLT_URL);
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
            InputStream stream = new ByteArrayInputStream(fileContent);
            Source xmlDoc = new StreamSource(stream);
            StringWriter writer = new StringWriter();
            transformer.transform(xmlDoc, new StreamResult(writer));
            LOGGER.info("XsltBolt: transformation success for: {}", fileUrl);
            t.setFileData(writer.toString().getBytes(Charset.forName("UTF-8")));
            outputCollector.emit(inputTuple, t.toStormTuple());
        } catch (TransformerConfigurationException e) {
            LOGGER.error("XsltBolt error:" + e.getMessage());
            emitDropNotification(t.getTaskId(), "", e.getMessage(), t
                    .getParameters().toString());
        } catch (TransformerException e) {
            LOGGER.error("XsltBolt error:" + e.getMessage());
            emitDropNotification(t.getTaskId(), "", e.getMessage(), t
                    .getParameters().toString());
        } catch (Exception e) {
            LOGGER.error("XsltBolt error:" + e.getMessage());
            emitDropNotification(t.getTaskId(), "", e.getMessage(), t
                    .getParameters().toString());
        }
    }

    @Override
    public void prepare() {
    }
}

