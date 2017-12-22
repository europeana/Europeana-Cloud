package eu.europeana.cloud.dps.topologies.media;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

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
import eu.europeana.cloud.service.mcs.exception.MCSException;

public class FileUrlSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private boolean alreadyEmitted = false;
	
	private static final Logger logger = LoggerFactory.getLogger(FileUrlSpout.class);
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void nextTuple() {
		if (!alreadyEmitted) {
			DataSetServiceClient datasetClient = new DataSetServiceClient(
					"https://test-cloud.europeana.eu/api", "mms_user",
					"pass");
			try {
				String nextSlice = null;
				do {
					ResultSlice<Representation> data =
							datasetClient.getDataSetRepresentationsChunk("mms_prov", "mms_set", nextSlice);
					nextSlice = data.getNextSlice();
					emitData(data.getResults());
					
				} while (nextSlice != null);
				
			} catch (Exception e) {
				logger.error("Emiting data failure", e);
			}
			
			alreadyEmitted = true;
		}
	}
	
	private void emitData(List<Representation> res)
			throws MCSException, IOException,
			ParserConfigurationException, SAXException {
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		
		for (Representation rep : res) {
			if (!rep.getRepresentationName().equals("edm"))
				continue;
			
			FileServiceClient fc = new FileServiceClient("https://test-cloud.europeana.eu/api",
					"mms_user", "pass");
			
			for (File representationFile : rep.getFiles()) {
				try (InputStream is =
						fc.getFile(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(),
								representationFile.getFileName())) {
					
					Document document = dBuilder.parse(is);
					emitFile("edm:object", document);
					emitFile("edm:hasView", document);
					emitFile("edm:isShownBy", document);
				}
				
			}
			
		}
	}
	
	private void emitFile(String tagName, Document document) {
		NodeList ob = document.getElementsByTagName(tagName);
		if (ob.getLength() > 0) {
			collector.emit(new Values(tagName + ";" + ((Element) ob.item(0)).getAttribute("rdf:resource")));
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("fileUrl"));
	}
	
}
