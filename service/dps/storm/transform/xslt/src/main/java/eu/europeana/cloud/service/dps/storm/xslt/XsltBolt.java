package eu.europeana.cloud.service.dps.storm.xslt;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;

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

		String file = null;
		String fileUrl = null;
		String xsltUrl = null;

		try {

			fileUrl = t.getFileUrl();
			file = t.getFileByteData();
			xsltUrl = t.getParameter(PluginParameterKeys.XSLT_URL);

			LOGGER.info("processing file: {}", fileUrl);
			LOGGER.debug("xslt schema:" + xsltUrl);

		} catch (Exception e) {
			LOGGER.error("XsltBolt error:" + e.getMessage());
			emitDropNotification(t.getTaskId(), "", e.getMessage(), t
					.getParameters().toString());
			emitBasicInfo(t.getTaskId(), 1);
			outputCollector.ack(inputTuple);
		}

		Source xslDoc = null;
		Source xmlDoc = null;

		try {
			xslDoc = new StreamSource(new URL(xsltUrl).openStream());
			InputStream stream = new ByteArrayInputStream(file.getBytes());
			xmlDoc = new StreamSource(stream);
		} catch (IOException e) {
			LOGGER.error("XsltBolt error:" + e.getMessage());
			emitDropNotification(t.getTaskId(), "", e.getMessage(), t
					.getParameters().toString());
			emitBasicInfo(t.getTaskId(), 1);
			outputCollector.ack(inputTuple);
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
				emitBasicInfo(t.getTaskId(), 1);
				outputCollector.ack(inputTuple);
			}
		}

		try {
			transformer.transform(xmlDoc, new StreamResult(writer));
		} catch (TransformerException e) {
			LOGGER.error("XsltBolt error:" + e.getMessage());
			emitDropNotification(t.getTaskId(), "", e.getMessage(), t
					.getParameters().toString());
			emitBasicInfo(t.getTaskId(), 1);
			outputCollector.ack(inputTuple);
		}

		LOGGER.info("XsltBolt: transformation success for: {}", fileUrl);

		// pass data to next Bolt
		t.setFileData(writer.toString());
		// outputCollector.emit(t.toStormTuple());
		emitBasicInfo(t.getTaskId(), 1);
		outputCollector.emit(inputTuple, t.toStormTuple());
		outputCollector.ack(inputTuple);
	}

	@Override
	public void prepare() {
	}
}
