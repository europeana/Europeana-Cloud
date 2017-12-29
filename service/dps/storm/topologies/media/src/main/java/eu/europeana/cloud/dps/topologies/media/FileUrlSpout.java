package eu.europeana.cloud.dps.topologies.media;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.MCSException;

public class FileUrlSpout extends BaseRichSpout implements TupleConstants {
	
	private static final String CONF_FS_URL = "MEDIATOPOLOGY_FILE_SERVICE_URL";
	private static final String CONF_FS_USER = "MEDIATOPOLOGY_FILE_SERVICE_USER";
	private static final String CONF_FS_PASS = "MEDIATOPOLOGY_FILE_SERVICE_PASSWORD";
	private static final String CONF_FS_PROVIDER = "MEDIATOPOLOGY_FILE_SERVICE_PROVIDER";
	private static final String CONF_FS_DATASET = "MEDIATOPOLOGY_FILE_SERVICE_DATASET";
	
	private SpoutOutputCollector collector;
	
	private DataSetServiceClient datasetClient;
	private ArrayDeque<Representation> currentSliceResults = new ArrayDeque<>();
	private String datasetProvider, datasetId;
	private String nextSliceId;
	private FileServiceClient fileClient;
	
	private DocumentBuilder documentBuilder;
	
	private static final Logger logger = LoggerFactory.getLogger(FileUrlSpout.class);
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		
		Map<String, String> config = conf;
		datasetClient = new DataSetServiceClient(config.get(CONF_FS_URL), config.get(CONF_FS_USER),
				config.get(CONF_FS_PASS));
		datasetProvider = config.get(CONF_FS_PROVIDER);
		datasetId = config.get(CONF_FS_DATASET);
		retrieveSlice();
		
		fileClient = new FileServiceClient(config.get(CONF_FS_URL), config.get(CONF_FS_USER), config.get(CONF_FS_PASS));
		
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			documentBuilder = dbFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new RuntimeException("xml problem", e);
		}
	}
	
	@Override
	public void nextTuple() {
		while (currentSliceResults.isEmpty()) {
			if (nextSliceId != null) {
				retrieveSlice();
			} else {
				// everything retrieved
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				return;
			}
		}
		
		List<UrlType> urlTypes = Arrays.asList(UrlType.OBJECT, UrlType.HAS_VIEW, UrlType.IS_SHOWN_BY);
		Representation rep = currentSliceResults.remove();
		for (File file : rep.getFiles()) {
			Document document;
			long start = System.currentTimeMillis();
			try (InputStream is = fileClient.getFile(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(),
					file.getFileName())) {
				document = documentBuilder.parse(is);
			} catch (MCSException | DriverException | IOException | SAXException e) {
				logger.error("Could not read file " + file + " in representation " + rep, e);
				return;
			}
			logger.debug("representation file downloaded and parsed in {} ms", System.currentTimeMillis() - start);
			for (UrlType urlType : urlTypes) {
				NodeList list = document.getElementsByTagName(urlType.tagName);
				if (list.getLength() > 0) {
					String url = ((Element) list.item(0)).getAttribute("rdf:resource");
					collector.emit(new Values(url, urlType));
				}
			}
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(URL, URL_TYPE));
	}
	
	private void retrieveSlice() {
		try {
			long start = System.currentTimeMillis();
			ResultSlice<Representation> data =
					datasetClient.getDataSetRepresentationsChunk(datasetProvider, datasetId, nextSliceId);
			logger.debug("dataSet slice downloaded in {} ms", System.currentTimeMillis() - start);
			currentSliceResults.addAll(data.getResults().stream()
					.filter(r -> "edm".equals(r.getRepresentationName()))
					.collect(Collectors.toList()));
			nextSliceId = data.getNextSlice();
		} catch (MCSException e) {
			throw new RuntimeException("File service connection error", e);
		}
	}
	
}
