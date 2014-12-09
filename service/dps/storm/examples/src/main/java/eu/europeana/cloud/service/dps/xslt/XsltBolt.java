package eu.europeana.cloud.service.dps.xslt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

import javax.xml.transform.Result;
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

		// downloading XSLT from URL...
		URL xslt = null;
		try {
			xslt = new URL(xsltUrl);
		} catch (MalformedURLException e) {
			LOGGER.error("XsltBolt error:" + e.getMessage());
		}

		URLConnection connection = null;
		try {
			connection = xslt.openConnection();
		} catch (IOException e) {
			LOGGER.error("XsltBolt error:" + e.getMessage());
		}

		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(
					connection.getInputStream()));
		} catch (IOException e) {
			LOGGER.error("XsltBolt error:" + e.getMessage());
		}

		StringBuilder response = new StringBuilder();
		String inputLine;

		try {
			while ((inputLine = in.readLine()) != null)
				response.append(inputLine);
			in.close();
		} catch (IOException e) {
			LOGGER.error("XsltBolt error:" + e.getMessage());
		}

		String xsltSheet = response.toString();
		Source xslDoc = new StreamSource(xsltSheet);
		Source xmlDoc = new StreamSource(file);
		TransformerFactory tFactory = TransformerFactory.newInstance();
		Transformer transformer = null;
		StreamResult out = new StreamResult();

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
			transformer.transform(xmlDoc, out);
		} catch (TransformerException e) {
			System.out.println("xslt error:" + e.getMessage());
			LOGGER.error("XsltBolt error:" + e.getMessage());
		}

		collector.emit(new Values(fileUrl, out.getWriter().toString(), t.getParameters()));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}
}
