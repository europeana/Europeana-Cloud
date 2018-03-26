package eu.europeana.cloud.dps.topologies.media;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.shade.org.eclipse.jetty.util.ConcurrentHashSet;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.StatsInitTupleData;
import eu.europeana.cloud.dps.topologies.media.support.Util;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.metis.mediaservice.EdmObject;
import eu.europeana.metis.mediaservice.UrlType;

/**
 * Wraps a base spout that generates tuples with JSON encoded {@link DpsTask}s
 * (usually a {@link KafkaSpout}) and translates its output to handle one EDM
 * representation per tuple.
 */
public class DataSetReaderSpout extends BaseRichSpout {
	
	public static final String SOURCE_FIELD = "source";
	
	private static final Logger logger = LoggerFactory.getLogger(DataSetReaderSpout.class);
	
	private final IRichSpout baseSpout;
	private final Collection<UrlType> urlTypes;
	private SpoutOutputCollector outputCollector;
	private Map<String, Object> config;
	
	private ConcurrentHashMap<String, EdmInfo> edmsByCloudId = new ConcurrentHashMap<>();
	
	private ConcurrentHashMap<String, SourceInfo> sourcesByHost = new ConcurrentHashMap<>();
	
	private DatasetDownloader datasetDownloader;
	private EdmDownloader edmDownloader;
	
	private long emitLimit;
	
	public DataSetReaderSpout(IRichSpout baseSpout, Collection<UrlType> urlTypes) {
		this.baseSpout = baseSpout;
		this.urlTypes = urlTypes;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(MediaTupleData.FIELD_NAME, SOURCE_FIELD));
		declarer.declareStream(StatsInitTupleData.STREAM_ID, new Fields(StatsInitTupleData.FIELD_NAME));
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		outputCollector = collector;
		config = conf;
		
		datasetDownloader = new DatasetDownloader();
		edmDownloader = new EdmDownloader();
		
		emitLimit = (Long) conf.getOrDefault("MEDIATOPOLOGY_DATASET_EMIT_LIMIT", Long.MAX_VALUE);
		
		baseSpout.open(conf, context, new CollectorWrapper(collector));
	}
	
	@Override
	public void close() {
		baseSpout.close();
		edmDownloader.stop();
	}
	
	@Override
	public void nextTuple() {
		baseSpout.nextTuple();
		Optional<SourceInfo> leastBusyActiveSource = sourcesByHost.values().stream()
				.filter(s -> !s.isEmpty())
				.min(Comparator.comparing(s -> s.running.size()));
		if (leastBusyActiveSource.isPresent()) {
			SourceInfo source = leastBusyActiveSource.get();
			EdmInfo edmInfo = source.removeFromQueue();
			TaskInfo taskInfo = edmInfo.taskInfo;
			long taskId = taskInfo.task.getTaskId();
			
			MediaTupleData tupleData = new MediaTupleData(taskId, edmInfo.representation);
			tupleData.setEdm(edmInfo.edmObject);
			tupleData.setFileInfos(edmInfo.fileInfos);
			tupleData.setConnectionLimitsPerSource(taskInfo.connectionLimitPerSource);
			
			outputCollector.emit(new Values(tupleData, source.host), edmInfo.representation.getCloudId());
			
			taskInfo.running.add(edmInfo);
			taskInfo.emitCount++;
			if (taskInfo.emitCount == taskInfo.emitsTotal) {
				outputCollector.emit(StatsInitTupleData.STREAM_ID,
						new Values(new StatsInitTupleData(taskId, taskInfo.startTime, taskInfo.emitsTotal)));
			}
			
			source.running.add(edmInfo);
		} else {
			// nothing to do
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}
	
	@Override
	public void ack(Object msgId) {
		EdmInfo edmInfo = edmsByCloudId.get(msgId);
		if (edmInfo == null) {
			logger.warn("Unrecognied ACK: {}", msgId);
			return;
		}
		
		removeEdm(edmInfo);
	}
	
	@Override
	public void fail(Object msgId) {
		EdmInfo edmInfo = edmsByCloudId.get(msgId);
		if (edmInfo == null) {
			logger.warn("Unrecognied FAIL: {}", msgId);
			return;
		}
		
		if (edmInfo.attempts > 0) {
			logger.info("FAIL received for {}, will retry ({} attempts left)", msgId, edmInfo.attempts);
			edmInfo.attempts--;
			edmInfo.sourceInfo.addToQueue(edmInfo);
		} else {
			logger.info("FAIL received for {}, no more retries", msgId);
			removeEdm(edmInfo);
		}
	}
	
	private void removeEdm(EdmInfo edmInfo) {
		SourceInfo source = edmInfo.sourceInfo;
		source.running.remove(edmInfo);
		if (source.running.isEmpty() && source.isEmpty()) {
			logger.info("Finished all EDMs from source {}", source.host);
			sourcesByHost.remove(source.host);
		}
		
		TaskInfo task = edmInfo.taskInfo;
		task.running.remove(edmInfo);
		if (task.running.isEmpty() && task.emitCount == task.emitsTotal) {
			logger.info("Task {} finished", task.task.getTaskName());
			baseSpout.ack(task.baseMsgId);
		}
	}
	
	private static class TaskInfo {
		DpsTask task;
		Map<String, Integer> connectionLimitPerSource;
		Object baseMsgId;
		List<UrlParser> datasetUrls = new ArrayList<>();
		ConcurrentHashSet<EdmInfo> running = new ConcurrentHashSet<>();
		int emitCount = 0;
		int emitsTotal;
		long startTime;
	}
	
	private static class SourceInfo {
		final String host;
		private ArrayDeque<EdmInfo> queue = new ArrayDeque<>();
		ConcurrentHashSet<EdmInfo> running = new ConcurrentHashSet<>();
		
		public SourceInfo(String host) {
			this.host = host;
		}
		
		public synchronized void addToQueue(EdmInfo edmInfo) {
			queue.add(edmInfo);
		}
		
		public synchronized EdmInfo removeFromQueue() {
			return queue.remove();
		}
		
		public synchronized boolean isEmpty() {
			return queue.isEmpty();
		}
	}
	
	private static class EdmInfo {
		Representation representation;
		EdmObject edmObject;
		List<FileInfo> fileInfos;
		TaskInfo taskInfo;
		SourceInfo sourceInfo;
		int attempts = 5;
	}
	
	private class EdmDownloader {
		
		ArrayBlockingQueue<EdmInfo> edmQueue = new ArrayBlockingQueue<>(1024 * 128);
		ArrayList<EdmDownloadThread> threads;
		
		public EdmDownloader() {
			int threadsCount = (int) config.getOrDefault("MEDIATOPOLOGY_REPRESENTATION_DOWNLOAD_THREADS", 1);
			threads = new ArrayList<>();
			for (int i = 0; i < threadsCount; i++) {
				EdmDownloadThread thread = new EdmDownloadThread(i);
				thread.start();
				threads.add(thread);
			}
		}
		
		public void queue(EdmInfo edmInfo) throws InterruptedException {
			edmQueue.put(edmInfo);
		}
		
		public void stop() {
			for (EdmDownloadThread thread : threads)
				thread.interrupt();
		}
		
		private class EdmDownloadThread extends Thread {
			
			FileServiceClient fileClient;
			EdmObject.Parser parser;
			
			public EdmDownloadThread(int id) {
				super("edm-downloader-" + id);
				fileClient = Util.getFileServiceClient(config);
				parser = new EdmObject.Parser();
			}
			
			@Override
			public void run() {
				try {
					while (true) {
						EdmInfo edmInfo = edmQueue.take();
						edmInfo.edmObject = downloadEdm(edmInfo);
						prepareEdmInfo(edmInfo);
					}
				} catch (InterruptedException e) {
					logger.trace("edm download thread finishing", e);
					Thread.currentThread().interrupt();
				}
			}
			
			EdmObject downloadEdm(EdmInfo edmInfo) throws InterruptedException {
				Representation rep = edmInfo.representation;
				try {
					File file = rep.getFiles().get(0);
					try (InputStream is = fileClient.getFile(rep.getCloudId(), rep.getRepresentationName(),
							rep.getVersion(), file.getFileName())) {
						return parser.parseXml(is);
					}
				} catch (Exception e) {
					logger.error("Downloading edm failed for " + rep, e);
					edmQueue.put(edmInfo); // retry later
					Thread.sleep(1000);
					return null;
				}
			}
			
			void prepareEdmInfo(EdmInfo edmInfo) {
				Set<String> urls = edmInfo.edmObject.getResourceUrls(urlTypes).keySet();
				if (urls.isEmpty()) {
					logger.error("content url missing in edm file representation: " + edmInfo.representation);
					return;
				}
				edmInfo.fileInfos = urls.stream().map(FileInfo::new).collect(Collectors.toList());
				
				Map<String, Long> hostCounts = urls.stream()
						.collect(Collectors.groupingBy(url -> URI.create(url).getHost(), Collectors.counting()));
				Comparator<Entry<String, Long>> c = Comparator.comparing(Entry::getValue);
				c = c.thenComparing(Entry::getKey); // compiler weirdness, can't do it in one call
				String host = hostCounts.entrySet().stream().max(c).get().getKey();
				edmInfo.sourceInfo = sourcesByHost.computeIfAbsent(host, SourceInfo::new);
				edmInfo.sourceInfo.addToQueue(edmInfo);
			}
		}
	}
	
	private class DatasetDownloader extends Thread {
		
		ArrayBlockingQueue<TaskInfo> taskQueue = new ArrayBlockingQueue<>(128);
		DataSetServiceClient datasetClient;
		
		public DatasetDownloader() {
			super("dataset-downloader");
			datasetClient = Util.getDataSetServiceClient(config);
			start();
		}
		
		public void queue(TaskInfo taskInfo) {
			try {
				taskQueue.put(taskInfo);
			} catch (InterruptedException e) {
				logger.trace("Thread interrupted", e);
				Thread.currentThread().interrupt();
			}
		}
		
		@Override
		public void run() {
			try {
				while (true) {
					TaskInfo taskInfo = null;
					try {
						taskInfo = taskQueue.take();
						downloadDataset(taskInfo);
					} catch (DriverException e) {
						logger.warn("Problem downloading datasets from task " + taskInfo.task.getTaskName(), e);
						taskQueue.put(taskInfo); // retry later
						Thread.sleep(1000);
					}
				}
			} catch (InterruptedException e) {
				logger.trace("dataset download thread finishing", e);
				Thread.currentThread().interrupt();
			}
		}
		
		void downloadDataset(TaskInfo taskInfo) throws InterruptedException {
			int edmCount = 0;
			for (UrlParser datasetUrl : taskInfo.datasetUrls) {
				RepresentationIterator representationIterator = datasetClient.getRepresentationIterator(
						datasetUrl.getPart(UrlPart.DATA_PROVIDERS),
						datasetUrl.getPart(UrlPart.DATA_SETS));
				while (representationIterator.hasNext() && edmCount < emitLimit) {
					Representation rep = representationIterator.next();
					if (!"edm".equals(rep.getRepresentationName()))
						continue;
					EdmInfo edmInfo = new EdmInfo();
					edmInfo.representation = rep;
					edmInfo.taskInfo = taskInfo;
					edmsByCloudId.put(rep.getCloudId(), edmInfo);
					edmDownloader.queue(edmInfo);
					edmCount++;
				}
			}
			taskInfo.emitsTotal = edmCount;
			logger.debug("Downloaded {} representations from datasets {}", edmCount, taskInfo.datasetUrls.stream()
					.map(du -> du.getPart(UrlPart.DATA_SETS)).collect(Collectors.toList()));
		}
	}
	
	private class CollectorWrapper extends SpoutOutputCollector {
		
		public CollectorWrapper(ISpoutOutputCollector delegate) {
			super(delegate);
		}
		
		@Override
		public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
			try {
				DpsTask task = new ObjectMapper().readValue((String) tuple.get(0), DpsTask.class);
				TaskInfo taskInfo = new TaskInfo();
				taskInfo.task = task;
				taskInfo.baseMsgId = messageId;
				taskInfo.startTime = System.currentTimeMillis();
				for (String datasetUrl : task.getDataEntry(InputDataType.DATASET_URLS))
					taskInfo.datasetUrls.add(new UrlParser(datasetUrl));
				
				taskInfo.connectionLimitPerSource = task.getParameters().entrySet().stream()
						.filter(e -> e.getKey().startsWith("host.limit."))
						.collect(Collectors.toMap(e -> e.getKey().replace("host.limit.", ""),
								e -> Integer.valueOf(e.getValue())));
				
				datasetDownloader.queue(taskInfo);
				logger.info("Task {} parsed", task.getTaskName());
			} catch (IOException e) {
				logger.error("Task rejected ({}: {})\n{}", e.getClass().getSimpleName(), e.getMessage(), tuple.get(0));
				logger.debug("Exception details", e);
			}
			
			return Collections.emptyList();
		}
	}
}
