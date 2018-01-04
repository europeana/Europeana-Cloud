package eu.europeana.cloud.dps.topologies.media;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.dps.topologies.media.MediaTupleData.UrlType;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;

public class ProcessingBolt extends BaseRichBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(ProcessingBolt.class);
	
	private ExecutorService threadPool;
	
	private FileServiceClient fileClient;
	
	private RecordServiceClient recordClient;
	
	private String identifyCmd;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		identifyCmd = findImageMagickCommand("identify");
		threadPool = Executors.newFixedThreadPool(2);
		fileClient = Util.getFileServiceClient(stormConf);
		recordClient = Util.getRecordServiceClient(stormConf);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// nothing to declare
	}
	
	@Override
	public void execute(Tuple input) {
		MediaTupleData mediaData = (MediaTupleData) input.getValueByField(MediaTupleData.FIELD_NAME);
		
		Representation edmRep = mediaData.getEdmRepresentation();
		HashSet<String> processedUrls = new HashSet<>();
		for (Entry<UrlType, String> entry : mediaData.getFileUrls().entrySet()) {
			String url = entry.getValue();
			if (!processedUrls.add(url))
				continue;
			try {
				Process identifyProcess = runCommand(Arrays.asList(identifyCmd, "-verbose", "-"), false,
						mediaData.getFileContents().get(url));
				String identifyResult = doAndClose(IOUtils::toString, identifyProcess.getInputStream());
				logger.debug("identify result:\n{}", identifyResult);
				
				storeRepresentation(edmRep.getCloudId(), edmRep.getDataProvider(), "techmetadata",
						identifyResult.getBytes("UTF-8"), "text/plain");
			} catch (IOException e) {
				logger.error("Image Magick identify: I/O error on " + edmRep, e);
			} catch (MediaException e) {
				logger.error("Processing url " + url + " failed", e);
			}
		}
	}
	
	private String findImageMagickCommand(String command) {
		boolean isWindows = System.getProperty("os.name").toLowerCase().contains("win");
		try {
			Process which = runCommand(Arrays.asList(isWindows ? "where" : "which", command), true);
			List<String> paths = doAndClose(IOUtils::readLines, which.getInputStream());
			for (String path : paths) {
				Process test = runCommand(Arrays.asList(path, "-version"), true);
				String version = doAndClose(IOUtils::toString, test.getInputStream());
				if (version.matches("(?s)^Version: ImageMagick (6|7).*")) // (?s) means . matches newline
					return path;
			}
			throw new RuntimeException("ImageMagick command " + command + " version 6/7 not found in " + paths);
		} catch (IOException e) {
			throw new RuntimeException("Error while looking for ImageMagick tools", e);
		}
	}
	
	private Process runCommand(List<String> command, boolean mergeError) throws IOException {
		return runCommand(command, mergeError, null);
	}
	
	private Process runCommand(List<String> command, boolean mergeError, byte[] inputBytes) throws IOException {
		Process process = new ProcessBuilder(command).redirectErrorStream(mergeError).start();
		if (!mergeError) {
			threadPool.execute(() -> {
				try (InputStream errorStream = process.getErrorStream()) {
					String output = IOUtils.toString(errorStream);
					if (!StringUtils.isBlank(output))
						logger.warn("Command: {}\nerror output:\n{}", command, output);
				} catch (IOException e) {
					logger.error("Error stream reading faild for command " + command, e);
				}
			});
		}
		if (inputBytes != null) {
			threadPool.execute(() -> {
				try (OutputStream processInput = process.getOutputStream()) {
					processInput.write(inputBytes);
				} catch (IOException e) {
					logger.error("Pushing data to process input stream failed for command " + command, e);
				}
			});
		}
		return process;
	}
	
	private void storeRepresentation(String cloudId, String providerId, String repName, byte[] data, String mimeType)
			throws MediaException {
		try {
			long start = System.currentTimeMillis();
			URI uri;
			try {
				uri = recordClient.getRepresentations(cloudId, repName).stream()
						.filter(r -> !r.isPersistent())
						.findAny().get().getUri();
				logger.debug("overwriting existing representation: " + uri);
			} catch (RepresentationNotExistsException | NoSuchElementException e) {
				uri = recordClient.createRepresentation(cloudId, repName, providerId);
				logger.debug("saving new representation: " + uri);
			}
			try (ByteArrayInputStream bais = new ByteArrayInputStream(data)) {
				URI newFileUri = fileClient.uploadFile(uri.toString(), bais, mimeType);
				logger.debug("saved representation in {} ms: {}", System.currentTimeMillis() - start, newFileUri);
			}
		} catch (IOException | DriverException | MCSException e) {
			throw new MediaException("Could not store representation " + cloudId + "/" + repName, e);
		}
	}
	
	@FunctionalInterface
	private interface IOFunction<T, R> {
		R apply(T t) throws IOException;
	}
	
	private static <T extends Closeable, R> R doAndClose(IOFunction<T, R> callee, T input) throws IOException {
		try {
			return callee.apply(input);
		} finally {
			input.close();
		}
	}
}
