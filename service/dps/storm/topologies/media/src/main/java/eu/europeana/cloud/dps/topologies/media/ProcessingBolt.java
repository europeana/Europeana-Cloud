package eu.europeana.cloud.dps.topologies.media;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.dps.topologies.media.support.MediaException;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.UrlType;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.dps.topologies.media.support.Util;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.mcs.exception.MCSException;

public class ProcessingBolt extends BaseRichBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(ProcessingBolt.class);
	
	// careful - in some places it's assumed these won't change
	private static final int[] THUMB_SIZE = { 200, 400 };
	
	private OutputCollector outputCollector;
	
	private ExecutorService threadPool;
	private Transformer xmlTtransformer;
	
	private FileServiceClient fileClient;
	
	private RecordServiceClient recordClient;
	
	private boolean persistResult;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.outputCollector = collector;
		threadPool = Executors.newFixedThreadPool(2);
		try {
			xmlTtransformer = TransformerFactory.newInstance().newTransformer();
		} catch (TransformerConfigurationException | TransformerFactoryConfigurationError e) {
			throw new AssertionError(e);
		}
		fileClient = Util.getFileServiceClient(stormConf);
		recordClient = Util.getRecordServiceClient(stormConf);
		
		persistResult = (Boolean) stormConf.getOrDefault("MEDIATOPOLOGY_RESULT_PERSIST", true);
		
		ImageInfo.init(this);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(StatsTupleData.STREAM_ID, new Fields(StatsTupleData.FIELD_NAME));
	}
	
	@Override
	public void execute(Tuple input) {
		MediaTupleData mediaData = (MediaTupleData) input.getValueByField(MediaTupleData.FIELD_NAME);
		StatsTupleData statsData = (StatsTupleData) input.getValueByField(StatsTupleData.FIELD_NAME);
		
		ArrayList<MediaInfo> mediaInfos = new ArrayList<>();
		statsData.setProcessingStartTime(System.currentTimeMillis());
		for (FileInfo file : mediaData.getFileInfos()) {
			if (isImage(file.getMimeType())) {
				mediaInfos.add(new ImageInfo(this, file, mediaData.getEdm()));
			}
		}
		mediaInfos.forEach(MediaInfo::process);
		statsData.setProcessingEndTime(System.currentTimeMillis());
		
		statsData.setUploadStartTime(System.currentTimeMillis());
		saveThumbnails(mediaData, mediaInfos);
		saveMetadata(mediaData, mediaInfos);
		statsData.setUploadEndTime(System.currentTimeMillis());
		
		mediaInfos.forEach(MediaInfo::clenaup);
		
		mediaInfos.stream().map(i -> i.error).filter(Objects::nonNull).forEach(statsData::addError);
		outputCollector.emit(StatsTupleData.STREAM_ID, input, new Values(statsData));
		outputCollector.ack(input);
	}
	
	private void saveMetadata(MediaTupleData mediaData, ArrayList<MediaInfo> mediaInfos) {
		long start = System.currentTimeMillis();
		try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
			xmlTtransformer.transform(new DOMSource(mediaData.getEdm()), new StreamResult(byteStream));
			try (ByteArrayInputStream bais = new ByteArrayInputStream(byteStream.toByteArray())) {
				Representation r = mediaData.getEdmRepresentation();
				if (persistResult) {
					URI newFileUri = recordClient.createRepresentation(r.getCloudId(), "techmetadata",
							r.getDataProvider(), bais, "techmetadata.xml", "text/xml");
					logger.debug("saved tech metadata in {} ms: {}", System.currentTimeMillis() - start, newFileUri);
				} else {
					URI rep = recordClient.createRepresentation(r.getCloudId(), "techmetadata", r.getDataProvider());
					String version = new UrlParser(rep.toString()).getPart(UrlPart.VERSIONS);
					fileClient.uploadFile(r.getCloudId(), "techmetadata", version, "techmetadata.xml", bais,
							"text/xml");
					recordClient.deleteRepresentation(r.getCloudId(), "techmetadata", version);
					logger.debug("tech metadata saving simulation took {} ms", System.currentTimeMillis() - start);
				}
			}
		} catch (IOException | DriverException | MCSException | TransformerException e) {
			logger.error("Could not store tech metadata representation in "
					+ mediaData.getEdmRepresentation().getCloudId(), e);
			for (MediaInfo media : mediaInfos) {
				if (media.error == null)
					media.error = "TECH METADATA SAVING";
			}
		}
	}
	
	private void saveThumbnails(MediaTupleData mediaData, ArrayList<MediaInfo> mediaInfos) {
		long start = System.currentTimeMillis();
		String cloudId = mediaData.getEdmRepresentation().getCloudId();
		String providerId = mediaData.getEdmRepresentation().getDataProvider();
		String repName = "thumbnail";
		try {
			URI uri = recordClient.createRepresentation(cloudId, repName, providerId);
			UrlParser urlParser = new UrlParser(uri.toString());
			String version = urlParser.getPart(UrlPart.VERSIONS);
			
			boolean uploaded = false;
			for (MediaInfo media : mediaInfos) {
				uploaded |= uploadThumbnail(cloudId, repName, version, media);
			}
			if (!uploaded)
				logger.debug("No thumbnails were uploaded for {}", cloudId);
			if (persistResult && uploaded) {
				recordClient.persistRepresentation(cloudId, repName, version);
			} else {
				recordClient.deleteRepresentation(cloudId, repName, version);
			}
		} catch (DriverException | MCSException | MalformedURLException e) {
			logger.error("Could not store thumbnail representation in "
					+ mediaData.getEdmRepresentation().getCloudId(), e);
			for (MediaInfo media : mediaInfos) {
				if (media.error == null)
					media.error = "THUMBNAIL SAVING";
			}
		}
		logger.debug("thumbnail saving{} took {} ms", persistResult ? "" : " simulation",
				System.currentTimeMillis() - start);
	}

	private boolean uploadThumbnail(String cloudId, String repName, String version, MediaInfo media) {
		String md5 = DigestUtils.md5Hex(media.fileInfo.getUrl());
		String mimeType = "image/" + media.getThumbnailFormat();
		boolean uploaded = false;
		for (int i = 0; i < THUMB_SIZE.length; i++) {
			try {
				if (media.thumbnails[i] == null)
					continue;
				try (FileInputStream fileStream = new FileInputStream(media.thumbnails[i])) {
					String fileName = md5 + "-w" + THUMB_SIZE[i];
					URI uri = fileClient.uploadFile(cloudId, repName, version, fileName, fileStream, mimeType);
					logger.debug("thumbnail saved: {}", uri);
				}
				uploaded = true;
			} catch (DriverException | MCSException | IOException e) {
				logger.error("Could not save thumbnails for cloudId " + cloudId, e);
				if (media.error == null)
					media.error = "THUMBNAIL SAVING";
			}
		}
		return uploaded;
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
	
	public static boolean isImage(String mimeType) {
		return mimeType.equals("application/pdf") || mimeType.startsWith("image");
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
	
	private abstract static class MediaInfo {
		
		static final String EBUCORE_RUI = "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#";
		
		final ProcessingBolt pb;
		final FileInfo fileInfo;
		final Document edm;
		Element edmWebResource;
		
		final String errorPrefix;
		String error;
		
		File[] thumbnails = new File[THUMB_SIZE.length];
		
		MediaInfo(ProcessingBolt pb, FileInfo fileInfo, Document edm) {
			this.pb = pb;
			this.fileInfo = fileInfo;
			this.edm = edm;
			
			this.errorPrefix = getClass().getSimpleName().replace("Info", " ").toUpperCase();
		}
		
		boolean shouldProcessMetadata() {
			return Arrays.asList(UrlType.HAS_VIEW, UrlType.IS_SHOWN_BY).stream()
					.anyMatch(fileInfo.getTypes()::contains);
		}
		
		boolean shouldProcessThumbnails() {
			return Arrays.asList(UrlType.OBJECT, UrlType.HAS_VIEW, UrlType.IS_SHOWN_BY).stream()
					.anyMatch(fileInfo.getTypes()::contains);
		}
		
		public void process() {
			long start = System.currentTimeMillis();
			try {
				doProcess();
				if (shouldProcessMetadata()) {
					updateResourceMetadata();
				}
				if (shouldProcessThumbnails()
						&& Arrays.stream(thumbnails).anyMatch(file -> file == null || file.length() == 0)) {
					logger.warn("No thumbnail output for " + fileInfo.getUrl());
					error = error == null ? "THUMBNAIL ERROR" : error;
				}
			} catch (IOException e) {
				logger.error("Exception processing " + fileInfo.getUrl(), e);
				error = errorPrefix + "IOException";
			} catch (MediaException e) {
				logger.info("{}processing failed ({}) for {}", errorPrefix, e.getMessage(), fileInfo.getUrl());
				logger.trace("{}failure details:", errorPrefix, e);
				if (e.reportError != null) {
					error = errorPrefix + e.reportError;
				} else {
					logger.warn("Exception without proper report error:", e);
					error = errorPrefix + "UNKNOWN";
				}
			} finally {
				logger.debug("{}processing took {} ms", errorPrefix, System.currentTimeMillis() - start);
			}
		}
		
		void updateResourceMetadata() {
			NodeList nList = edm.getElementsByTagName("edm:WebResource");
			for (int i = 0; i < nList.getLength(); i++) {
				Element node = (Element) nList.item(i);
				if (node.getAttributes().getNamedItem("rdf:about").getNodeValue().equals(fileInfo.getUrl())) {
					edmWebResource = node;
					break;
				}
			}
			if (edmWebResource == null) {
				edmWebResource = edm.createElement("edm:WebResource");
				edmWebResource.setAttribute("rdf:about", fileInfo.getUrl());
				edm.getDocumentElement().appendChild(edmWebResource);
			}
			edm.getDocumentElement().setAttribute("xmlns:ebucore", EBUCORE_RUI);
		}
		
		void setEbucoreValue(String name, Object value) {
			Element ebucoreElement = (Element) edmWebResource.getElementsByTagName(name).item(0);
			if (ebucoreElement == null) {
				ebucoreElement = edm.createElementNS(EBUCORE_RUI, "ebucore:" + name);
				edmWebResource.appendChild(ebucoreElement);
				
				String type = value.getClass().getSimpleName();
				if (!"string".equalsIgnoreCase(type)) {
					ebucoreElement.setAttribute("rdf:datatype",
							"http://www.w3.org/2001/XMLSchema#" + StringUtils.lowerCase(type));
				}
			}
			ebucoreElement.setTextContent(String.valueOf(value));
		}
		
		public void clenaup() {
			for (File thumb : thumbnails) {
				if (thumb != null && !thumb.delete())
					logger.warn("Could not delete thumbnail from temp: {}", thumb);
			}
		}
		
		abstract void doProcess() throws IOException, MediaException;
		
		public abstract String getThumbnailFormat();
	}
	
	private static class ImageInfo extends MediaInfo {
		
		static final Pattern MIME_TYPE = Pattern.compile("^  Mime type: (.*)", Pattern.MULTILINE);
		
		static final Pattern GEOMETRY = Pattern.compile("^  Geometry: (\\d+)x(\\d+)", Pattern.MULTILINE);
		
		static final Pattern COLORSPACE = Pattern.compile("^  Colorspace: (.*)", Pattern.MULTILINE);
		
		static final Pattern ORIENTATION = Pattern.compile("^  Orientation: (.*)", Pattern.MULTILINE);
		
		static final Pattern DOMINANT_COLORS = Pattern.compile("^\\s+([0-9]*):.*(#[0-9a-zA-Z]+).*$", Pattern.MULTILINE);
		
		static String magickCmd;
		
		int width;
		int height;
		String colorspace;
		String orientation;
		
		List<String> dominantColors = new ArrayList<>();
		
		ImageInfo(ProcessingBolt pb, FileInfo fileInfo, Document edm) {
			super(pb, fileInfo, edm);
		}
		
		@Override
		void doProcess() throws IOException, MediaException {
			ArrayList<String> convertCmd = new ArrayList<>();
			convertCmd.addAll(Arrays.asList(magickCmd, "convert", "-", "-verbose", "-write", "info:", "+verbose"));
			if (shouldProcessThumbnails()) {
				if ("application/pdf".equals(fileInfo.getMimeType()))
					convertCmd.addAll(Arrays.asList("-background", "white", "-alpha", "remove"));
				String ext = "." + getThumbnailFormat();
				thumbnails[0] = File.createTempFile("thumb200", ext);
				thumbnails[1] = File.createTempFile("thumb400", ext);
				convertCmd.addAll(Arrays.asList(
						"(", "+clone", "-thumbnail", "200x", "-write", thumbnails[0].getPath(), "+delete", ")",
						"(", "+clone", "-thumbnail", "400x", "-write", thumbnails[1].getPath(), "+delete", ")"));
				
				convertCmd
						.addAll(Arrays.asList("+dither", "-colors", "6", "-define", "histogram:unique-colors=true",
								"-format", "\nDominant colors:\n%c", "histogram:info:"));
			}
			Process magickProcess = pb.runCommand(convertCmd, false, fileInfo.getContent());
			String identifyResult = doAndClose(IOUtils::toString, magickProcess.getInputStream());
			extract(identifyResult);
		}
		
		void extract(String identifyResult) throws MediaException {
			Matcher m = MIME_TYPE.matcher(identifyResult);
			Matcher g = GEOMETRY.matcher(identifyResult);
			Matcher c = COLORSPACE.matcher(identifyResult);
			Matcher o = ORIENTATION.matcher(identifyResult);
			if (m.find() && g.find() && c.find() && o.find()) {
				String mimeType = m.group(1);
				if (!mimeType.equals(fileInfo.getMimeType())) {
					throw new MediaException(
							"Mime type provided: " + fileInfo.getMimeType() + ", extracted: " + mimeType,
							"MIMETYPE INCONSISTENT");
				}
				width = Integer.parseInt(g.group(1));
				height = Integer.parseInt(g.group(2));
				colorspace = c.group(1);
				orientation = o.group(1);
				
				Matcher dc =
						DOMINANT_COLORS.matcher(identifyResult.substring(identifyResult.indexOf("Dominant colors:")));
				while (dc.find()) {
					dominantColors.add(dc.group(2));
				}
				
			} else {
				throw new MediaException("identify result missing data: " + identifyResult, "CORRUPTED");
			}
		}
		
		@Override
		public String getThumbnailFormat() {
			return Arrays.asList("application/pdf", "image/png").contains(fileInfo.getMimeType()) ? "png" : "jpeg";
		}
		
		@Override
		void updateResourceMetadata() {
			super.updateResourceMetadata();
			setEbucoreValue("hasMimeType", fileInfo.getMimeType());
			setEbucoreValue("fileByteSize", (long) fileInfo.getContent().length);
			setEbucoreValue("width", width);
			setEbucoreValue("height", height);
			setEbucoreValue("hasFormat", colorspace);
			setEbucoreValue("dominantColors", String.join(",", dominantColors));
			setEbucoreValue("orientation", orientation);
		}
		
		static synchronized void init(ProcessingBolt pb) {
			// make sure ImageMagick is available
			boolean isWindows = System.getProperty("os.name").toLowerCase().contains("win");
			try {
				Process which = pb.runCommand(Arrays.asList(isWindows ? "where" : "which", "magick"), true);
				List<String> paths = doAndClose(IOUtils::readLines, which.getInputStream());
				for (String path : paths) {
					Process test = pb.runCommand(Arrays.asList(path, "-version"), true);
					String version = doAndClose(IOUtils::toString, test.getInputStream());
					if (version.matches("(?s)^Version: ImageMagick (6|7).*")) { // (?s) means . matches newline
						magickCmd = path;
						return;
					}
				}
				throw new RuntimeException("ImageMagick command version 6/7 not found in " + paths);
			} catch (IOException e) {
				throw new RuntimeException("Error while looking for ImageMagick tools", e);
			}
		}
	}
}
