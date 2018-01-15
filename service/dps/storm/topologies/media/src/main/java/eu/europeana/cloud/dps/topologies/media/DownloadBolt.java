package eu.europeana.cloud.dps.topologies.media;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.dps.topologies.media.MediaTupleData.UrlType;
import eu.europeana.cloud.mcs.driver.FileServiceClient;

public class DownloadBolt extends BaseRichBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(DownloadBolt.class);
	
	private OutputCollector outputCollector;
	
	private FileServiceClient fileClient;
	
	private DocumentBuilder documentBuilder;
	
	private CloseableHttpClient httpClient;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
		
		fileClient = Util.getFileServiceClient(stormConf);
		
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			dbFactory.setNamespaceAware(true);
			documentBuilder = dbFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new RuntimeException("xml problem", e);
		}
		
		httpClient = HttpClients.createDefault();
	}
	
	@Override
	public void cleanup() {
		try {
			httpClient.close();
		} catch (IOException e) {
			logger.error("HttpClient could not close", e);
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(MediaTupleData.FIELD_NAME));
		declarer.declareStream(StatsTupleData.STREAM_ID, new Fields(StatsTupleData.FIELD_NAME));
	}
	
	@Override
	public void execute(Tuple input) {
		MediaTupleData data = (MediaTupleData) input.getValueByField(MediaTupleData.FIELD_NAME);
		
		Map<UrlType, List<String>> urls;
		try {
			Representation rep = data.getEdmRepresentation();
			List<File> files = rep.getFiles();
			if (files.size() != 1)
				throw new MediaException("EDM representation has " + files.size() + " files!");
			Document edm = getEdmDocument(rep, files.get(0));
			urls = retrieveUrls(edm);
			
			data.setEdm(edm);
			data.setFileUrls(urls);
		} catch (MediaException e) {
			logger.error("Could not retrieve media urls from representation " + data.getEdmRepresentation(), e);
			return;
		}
		
		HashSet<String> urlsSet = new HashSet<>();
		for (List<String> values : urls.values())
			urlsSet.addAll(values);
		
		HashMap<String, byte[]> contents = new HashMap<>();
		for (String url : urlsSet) {
			byte[] content = downloadFile(url, data.getTaskId());
			if (content != null)
				contents.put(url, content);
		}
		if (!contents.isEmpty()) {
			data.setFileContents(contents);
			outputCollector.emit(new Values(data));
		}
	}
	
	private Map<UrlType, List<String>> retrieveUrls(Document edm) throws MediaException {
		Map<UrlType, List<String>> urls = new HashMap<>();
		long start = System.currentTimeMillis();
		
		for (UrlType urlType : Arrays.asList(UrlType.OBJECT, UrlType.HAS_VIEW, UrlType.IS_SHOWN_BY)) {
			NodeList list = edm.getElementsByTagName(urlType.tagName);
			if (list.getLength() > 0) {
				List<String> typeValues = new ArrayList<>();
				for (int i = 0; i < list.getLength(); i++) {
					typeValues.add(((Element) list.item(i)).getAttribute("rdf:resource"));
				}
				urls.put(urlType, typeValues);
			}
		}
		
		logger.debug("edm document retrieving took {} ms", System.currentTimeMillis() - start);
		
		if (urls.isEmpty())
			throw new MediaException("edm file representation doesn't contain content urls");
		return urls;
	}
	
	private byte[] downloadFile(String fileUrl, long taskId) {
		long start = System.currentTimeMillis();
		byte[] bytes = null;
		String error = null;
		try {
			@SuppressWarnings("resource") // closing response will prevent connection reuse
			HttpResponse response = httpClient.execute(new HttpGet(fileUrl));
			try {
				int status = response.getStatusLine().getStatusCode();
				if (status >= 200 && status < 300) {
					bytes = EntityUtils.toByteArray(response.getEntity());
				} else {
					error = "STATUS CODE " + status;
					logger.info("Download failed, status code {} for {}", status, fileUrl);
				}
			} finally {
				EntityUtils.consume(response.getEntity());
			}
		} catch (IOException e) {
			error = e.getMessage();
			logger.info("Download failed, {}: {} for {}", e.getClass(), e.getMessage(), fileUrl);
			logger.debug("Download exception", e);
		}
		
		StatsTupleData tupleData;
		if (bytes != null) {
			long time = System.currentTimeMillis() - start;
			logger.debug("Downloaded file: {} (size: {}, time: {})", fileUrl, bytes.length, time);
			tupleData = new StatsTupleData(taskId, bytes.length, time);
		} else {
			tupleData = new StatsTupleData(taskId, error);
		}
		outputCollector.emit(StatsTupleData.STREAM_ID, new Values(tupleData));
		
		return bytes;
	}
	
	private Document getEdmDocument(Representation rep, File file) throws MediaException {
		try {
			try (InputStream is = fileClient.getFile(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(),
					file.getFileName())) {
				return documentBuilder.parse(is);
			}
		} catch (Exception e) {
			throw new MediaException("Could not read edm file", e);
		}
		
	}
}
