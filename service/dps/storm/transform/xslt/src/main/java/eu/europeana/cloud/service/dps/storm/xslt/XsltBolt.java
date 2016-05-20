package eu.europeana.cloud.service.dps.storm.xslt;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
            100);

    @Override
    public void execute(StormTaskTuple t) {

        byte[] fileContent = null;
        String fileUrl = null;
        String xsltUrl = null;

        try {

            fileUrl = t.getFileUrl();
            fileContent = t.getFileData();
            xsltUrl = t.getParameter(PluginParameterKeys.XSLT_URL);

            LOGGER.info("processing file: {}", fileUrl);
            LOGGER.debug("xslt schema:" + xsltUrl);

        } catch (Exception e) {
            LOGGER.error("XsltBolt error:" + e.getMessage());
            emitDropNotification(t.getTaskId(), "", e.getMessage(), t
                    .getParameters().toString());
        }

        Source xslDoc = null;
        Source xmlDoc = null;

        try {
            xslDoc = new StreamSource(new URL(xsltUrl).openStream());
            InputStream stream = new ByteArrayInputStream(fileContent);
            xmlDoc = new StreamSource(stream);
        } catch (IOException e) {
            LOGGER.error("XsltBolt error:" + e.getMessage());
            emitDropNotification(t.getTaskId(), "", e.getMessage(), t
                    .getParameters().toString());
        }

        TransformerFactory tFactory = TransformerFactory.newInstance();
        Transformer transformer = null;
        StringWriter writer = new StringWriter();

        if (cache.containsKey(xsltUrl)) {
            transformer = cache.get(xsltUrl);
        } else {
            try {
                transformer = tFactory.newTransformer(xslDoc);
                cache.put(xsltUrl, transformer);
            } catch (TransformerConfigurationException e) {
                LOGGER.error("XsltBolt error:" + e.getMessage());
                emitDropNotification(t.getTaskId(), "", e.getMessage(), t
                        .getParameters().toString());
            }
        }

        try {
            transformer.transform(xmlDoc, new StreamResult(writer));
        } catch (TransformerException e) {
            LOGGER.error("XsltBolt error:" + e.getMessage());
            emitDropNotification(t.getTaskId(), "", e.getMessage(), t
                    .getParameters().toString());
        }

        LOGGER.info("XsltBolt: transformation success for: {}", fileUrl);

        // pass data to next Bolt
        t.setFileData(writer.toString().getBytes(Charset.forName("UTF-8")));
        // outputCollector.emit(t.toStormTuple());
        outputCollector.emit(inputTuple, t.toStormTuple());
    }

    @Override
    public void prepare() {
    }
}
