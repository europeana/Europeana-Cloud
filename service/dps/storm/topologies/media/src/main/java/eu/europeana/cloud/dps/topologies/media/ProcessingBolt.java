package eu.europeana.cloud.dps.topologies.media;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.dps.topologies.media.support.CancelChecker;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.dps.topologies.media.support.TempFileSync;
import eu.europeana.cloud.dps.topologies.media.support.Util;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.metis.mediaservice.EdmObject;
import eu.europeana.metis.mediaservice.MediaException;
import eu.europeana.metis.mediaservice.MediaProcessor;

public class ProcessingBolt extends BaseRichBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(ProcessingBolt.class);
	
	private static AmazonS3 amazonClient;
	private static String storageBucket = "";
	
	private OutputCollector outputCollector;
	private Map<String, Object> config;
	
	private MediaProcessor mediaProcessor;
	private ResultsUploader resultsUploader;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.outputCollector = collector;
		this.config = stormConf;
		
		mediaProcessor = new MediaProcessor();
		resultsUploader = new ResultsUploader();
		
		TempFileSync.init(stormConf);
		
		synchronized (ProcessingBolt.class) {
			if (amazonClient == null) {
				amazonClient = new AmazonS3Client(new BasicAWSCredentials(
						(String) config.get("AWS_CREDENTIALS_ACCESSKEY"),
						(String) config.get("AWS_CREDENTIALS_SECRETKEY")));
				amazonClient.setEndpoint((String) config.get("AWS_CREDENTIALS_ENDPOINT"));
				storageBucket = (String) config.get("AWS_CREDENTIALS_BUCKET");
			}
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(StatsTupleData.STREAM_ID, new Fields(StatsTupleData.FIELD_NAME));
	}
	
	@Override
	public void execute(Tuple input) {
		MediaTupleData mediaData = (MediaTupleData) input.getValueByField(MediaTupleData.FIELD_NAME);
		StatsTupleData statsData = (StatsTupleData) input.getValueByField(StatsTupleData.FIELD_NAME);
		
		statsData.setProcessingStartTime(System.currentTimeMillis());
		HashMap<String, String> errorsByUrl = new HashMap<>();
		EdmObject edm = mediaData.getEdm();
		mediaProcessor.setEdm(edm);
		for (FileInfo file : mediaData.getFileInfos()) {
			long start = System.currentTimeMillis();
			try {
				TempFileSync.ensureLocal(file);
				mediaProcessor.processResource(file.getUrl(), file.getMimeType(), file.getContent());
			} catch (MediaException e) {
				logger.info("processing failed ({}/{}) for {}", e.reportError, e.getMessage(), file.getUrl());
				logger.trace("failure details:", e);
				errorsByUrl.put(file.getUrl(), e.reportError);
				if (e.retry) {
					cleanupRecord(mediaData, mediaProcessor.getThumbnails());
					outputCollector.fail(input);
					return;
				}
				if (e.reportError == null) {
					logger.warn("Exception without proper report error:", e);
					errorsByUrl.put(file.getUrl(), "UNKNOWN");
				}
			} finally {
				logger.debug("Processing {} took {} ms", file.getUrl(), System.currentTimeMillis() - start);
			}
		}
		resultsUploader.queue(input, mediaData, statsData, errorsByUrl);
		statsData.setProcessingEndTime(System.currentTimeMillis());
		
	}
	
	@Override
	public void cleanup() {
		mediaProcessor.close();
		resultsUploader.stop();
	}
	
	private void cleanupRecord(MediaTupleData mediaData, Map<File, String> thumbnails) {
		for (FileInfo fileInfo : mediaData.getFileInfos()) {
			TempFileSync.delete(fileInfo);
		}
		for (File thumb : thumbnails.keySet()) {
			if (!thumb.delete())
				logger.warn("Could not delete thumbnail from temp: {}", thumb);
		}
	}
	
	private class ResultsUploader {
		
		class Item {
			Tuple tuple;
			StatsTupleData statsData;
			MediaTupleData mediaData;
			Map<File, String> thumbnails;
			byte[] edmContents;
			Map<String, String> errorsByUrl;
		}
		
		EdmObject.Writer edmWriter = new EdmObject.Writer();
		
		ArrayBlockingQueue<Item> queue = new ArrayBlockingQueue<>(100);
		ArrayList<ResultsUploadThread> threads = new ArrayList<>();
		
		boolean persistResult;
		
		public ResultsUploader() {
			persistResult = (Boolean) config.getOrDefault("MEDIATOPOLOGY_RESULT_PERSIST", true);
			
			int threadCount = (int) (long) config.getOrDefault("MEDIATOPOLOGY_RESULT_UPLOAD_THREADS", 2);
			for (int i = 0; i < threadCount; i++) {
				ResultsUploadThread thread = new ResultsUploadThread(i);
				thread.start();
				threads.add(thread);
			}
		}
		
		public void queue(Tuple tuple, MediaTupleData mediaData, StatsTupleData statsData,
				Map<String, String> errorsByUrl) {
			Item item = new Item();
			item.tuple = tuple;
			item.mediaData = mediaData;
			item.statsData = statsData;
			item.errorsByUrl = errorsByUrl;
			item.thumbnails = mediaProcessor.getThumbnails();
			item.edmContents = edmWriter.toXmlBytes(mediaProcessor.getEdm());
			try {
				if (queue.remainingCapacity() == 0)
					logger.warn("Results upload queue full");
				queue.put(item);
			} catch (InterruptedException e) {
				logger.trace("Thread interrupted", e);
				Thread.currentThread().interrupt();
			}
		}
		
		public void stop() {
			for (ResultsUploadThread thread : threads)
				thread.interrupt();
		}
		
		class ResultsUploadThread extends Thread {
			
			FileServiceClient fileClient;
			RecordServiceClient recordClient;
			
			Item currentItem;
			
			ResultsUploadThread(int id) {
				super("result-uploader-" + id);
				
				fileClient = Util.getFileServiceClient(config);
				recordClient = Util.getRecordServiceClient(config);
			}
			
			@Override
			public void run() {
				try {
					while (true) {
						StatsTupleData statsData;
						try {
							currentItem = queue.take();
							statsData = currentItem.statsData;
							
							statsData.setUploadStartTime(System.currentTimeMillis());
							saveThumbnails();
							saveMetadata();
							statsData.setUploadEndTime(System.currentTimeMillis());
						} finally {
							cleanupRecord(currentItem.mediaData, currentItem.thumbnails);
						}
						for (String error : currentItem.errorsByUrl.values())
							statsData.addError(error);
						outputCollector.emit(StatsTupleData.STREAM_ID, currentItem.tuple, new Values(statsData));
						outputCollector.ack(currentItem.tuple);
					}
				} catch (InterruptedException e) {
					logger.trace("result upload thread finishing", e);
					Thread.currentThread().interrupt();
					while ((currentItem = queue.poll()) != null) {
						cleanupRecord(currentItem.mediaData, currentItem.thumbnails);
					}
				}
			}
			
			void saveMetadata() {
				long start = System.currentTimeMillis();
				try (ByteArrayInputStream bais = new ByteArrayInputStream(currentItem.edmContents)) {
					if (currentItem.edmContents.length == 0)
						throw new IOException("Result EDM contents missing");
					
					Representation r = currentItem.mediaData.getEdmRepresentation();
					if (persistResult) {
						URI newFileUri = recordClient.createRepresentation(r.getCloudId(), "techmetadata",
								r.getDataProvider(), bais, "techmetadata.xml", "text/xml");
						logger.debug("saved tech metadata in {} ms: {}", System.currentTimeMillis() - start,
								newFileUri);
					} else {
						URI rep =
								recordClient.createRepresentation(r.getCloudId(), "techmetadata", r.getDataProvider());
						String version = new UrlParser(rep.toString()).getPart(UrlPart.VERSIONS);
						fileClient.uploadFile(r.getCloudId(), "techmetadata", version, "techmetadata.xml", bais,
								"text/xml");
						recordClient.deleteRepresentation(r.getCloudId(), "techmetadata", version);
						logger.debug("tech metadata saving simulation took {} ms", System.currentTimeMillis() - start);
					}
				} catch (IOException | DriverException | MCSException e) {
					logger.error("Could not store tech metadata representation in "
							+ currentItem.mediaData.getEdmRepresentation().getCloudId(), e);
					for (FileInfo file : currentItem.mediaData.getFileInfos()) {
						currentItem.errorsByUrl.putIfAbsent(file.getUrl(), "TECH METADATA SAVING");
					}
				}
			}
			
			void saveThumbnails() {
				long start = System.currentTimeMillis();
				boolean uploaded = false;
				for (Entry<File, String> entry : currentItem.thumbnails.entrySet()) {
					File thumbnail = entry.getKey();
					String url = entry.getValue();
					try {
						amazonClient.putObject(storageBucket, thumbnail.getName(), thumbnail);
						logger.debug("thumbnail saved: {} b, md5({}) = {}", thumbnail.length(), url,
								thumbnail.getName());
						uploaded = true;
					} catch (AmazonClientException e) {
						logger.error("Could not save thumbnails for " + url, e);
						currentItem.errorsByUrl.putIfAbsent(url, "THUMBNAIL SAVING");
					}
				}
				if (!uploaded) {
					logger.info("No thumbnails were uploaded for {}",
							currentItem.mediaData.getEdmRepresentation().getCloudId());
				}
				
				logger.debug("thumbnail saving took {} ms ", System.currentTimeMillis() - start);
			}
		}
	}
}
