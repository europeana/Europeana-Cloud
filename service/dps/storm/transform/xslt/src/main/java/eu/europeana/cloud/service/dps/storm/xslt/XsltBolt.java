package eu.europeana.cloud.service.dps.storm.xslt;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.util.Map;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import eu.europeana.cloud.service.dps.DpsKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTask;
import eu.europeana.cloud.service.dps.util.LRUCache;

public class XsltBolt extends AbstractDpsBolt {

	private OutputCollector collector;
	public static final Logger LOGGER = LoggerFactory.getLogger(XsltBolt.class);

	private LRUCache<String, Transformer> cache = new LRUCache<String, Transformer>(
			100);

	@Override
	public void execute(StormTask t) {

		String file = null;
		String fileUrl = null;
		String xsltUrl = null;

		try {

			fileUrl = t.getFileUrl();
			file = t.getFileByteData();
			xsltUrl = t.getParameter(DpsKeys.XSLT_URL);

			LOGGER.info("processing file: {}", fileUrl);
			LOGGER.debug("xslt schema:" + xsltUrl);

		} catch (Exception e) {
			LOGGER.error("XsltBolt error:" + e.getMessage());
		}

		Source xslDoc = null;
		Source xmlDoc = null;
		
		try {
			xslDoc = new StreamSource(new URL(xsltUrl).openStream());
			
			InputStream stream = new ByteArrayInputStream(file.getBytes());
			xmlDoc = new StreamSource(stream);
			
		} catch (IOException e1) {
			e1.printStackTrace();
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
			}
		}

		try {
			transformer.transform(xmlDoc, new StreamResult(writer));
		} catch (TransformerException e) {
			LOGGER.error("XsltBolt error:" + e.getMessage());
		}

		collector.emit(new Values(fileUrl, writer.toString(), t.getParameters()));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}
}
