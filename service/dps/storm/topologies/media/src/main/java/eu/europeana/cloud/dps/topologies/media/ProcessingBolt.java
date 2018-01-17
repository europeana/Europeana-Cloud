package eu.europeana.cloud.dps.topologies.media;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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

import eu.europeana.cloud.dps.topologies.media.support.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.MediaException;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.UrlType;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.dps.topologies.media.support.Util;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;

public class ProcessingBolt extends BaseRichBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(ProcessingBolt.class);
	
	private OutputCollector outputCollector;
	
	private ExecutorService threadPool;
	private Transformer xmlTtransformer;
	
	private FileServiceClient fileClient;
	
	private RecordServiceClient recordClient;
	
	private String magickCmd;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.outputCollector = collector;
		magickCmd = findImageMagickCommand();
		threadPool = Executors.newFixedThreadPool(2);
		try {
			xmlTtransformer = TransformerFactory.newInstance().newTransformer();
		} catch (TransformerConfigurationException | TransformerFactoryConfigurationError e) {
			throw new AssertionError(e);
		}
		fileClient = Util.getFileServiceClient(stormConf);
		recordClient = Util.getRecordServiceClient(stormConf);
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
		
		ArrayList<ImageInfo> imageInfos = new ArrayList<>();
		for (FileInfo file : mediaData.getFileInfos()) {
			if (!isImage(file.getMimeType()))
				continue;
			ImageInfo image = new ImageInfo(file);
			imageInfos.add(image);
			if (!identifyImage(image, mediaData.getEdm()))
				continue;
			if (image.shouldProcessThumbnails()) {
				image.thumbnail200 = createThumbnail(image, 200);
				image.thumbnail400 = createThumbnail(image, 400);
			}
		}
		saveThumbnails(mediaData, imageInfos);
		saveMetadata(mediaData, imageInfos);
		
		imageInfos.stream().map(i -> i.error).filter(Objects::nonNull).forEach(statsData::addError);
		statsData.setProcessingEndTime(System.currentTimeMillis());
		outputCollector.emit(StatsTupleData.STREAM_ID, input, new Values(statsData));
		outputCollector.ack(input);
	}
	
	private boolean identifyImage(ImageInfo image, Document edm) {
		try {
			Process identifyProcess = runCommand(Arrays.asList(magickCmd, "identify", "-verbose", "-"), false,
					image.fileInfo.getContent());
			String identifyResult = doAndClose(IOUtils::toString, identifyProcess.getInputStream());
			image.extract(identifyResult);
			if (image.shouldProcessMetadata()) {
				new EdmWebResourceWrapper(edm, image.fileInfo.getUrl()).setData(image);
			}
			return true;
		} catch (IOException e) {
			logger.error("Exception processing " + image.fileInfo.getUrl(), e);
			image.error = "IDENTIFY IOException";
			return false;
		} catch (MediaException e) {
			logger.info("Identify failed ({}) for {}", e.getMessage(), image.fileInfo.getUrl());
			logger.trace("Identify failure details:", e);
			if (e.reportError != null) {
				image.error = "IMAGE: " + e.reportError;
			} else {
				logger.warn("Exception without proper report error:", e);
				image.error = "IMAGE: UNKNOWN";
			}
			return false;
		}
	}
	
	private byte[] createThumbnail(ImageInfo image, int width) {
		String mimeType = image.fileInfo.getMimeType();
		if (image.width < width) {
			if (mimeType.equals("image/" + image.getThumbnailFormat()))
				return image.fileInfo.getContent();
			width = image.width;
		}
		ArrayList<String> convertCmd = new ArrayList<>();
		convertCmd.addAll(Arrays.asList(magickCmd, "convert", "-", "-thumbnail", width + "x"));
		if (mimeType.equals("application/pdf"))
			convertCmd.addAll(Arrays.asList("-background", "white", "-alpha", "remove"));
		convertCmd.add(image.getThumbnailFormat() + ":-");
		try {
			Process convertProcess = runCommand(convertCmd, false, image.fileInfo.getContent());
			byte[] result = doAndClose(IOUtils::toByteArray, convertProcess.getInputStream());
			if (result.length > 0)
				return result;
			logger.error("No convert output for " + image.fileInfo.getUrl());
			image.error = "THUMBNAIL ERROR";
			return null;
		} catch (IOException e) {
			logger.error("Exception processing " + image.fileInfo.getUrl(), e);
			image.error = "THUMBNAIL IOException";
			return null;
		}
	}
	
	private void saveMetadata(MediaTupleData mediaData, ArrayList<ImageInfo> imageInfos) {
		long start = System.currentTimeMillis();
		try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
			xmlTtransformer.transform(new DOMSource(mediaData.getEdm()), new StreamResult(byteStream));
			try (ByteArrayInputStream bais = new ByteArrayInputStream(byteStream.toByteArray())) {
				String representationUri = getRepresentationUri(mediaData, "techmetadata");
				URI newFileUri = fileClient.uploadFile(representationUri, bais, "text/xml");
				logger.debug("saved tech metadata in {} ms: {}", System.currentTimeMillis() - start, newFileUri);
			}
		} catch (IOException | DriverException | MCSException | TransformerException e) {
			logger.error("Could not store tech metadata representation in "
					+ mediaData.getEdmRepresentation().getCloudId(), e);
			for (ImageInfo image : imageInfos) {
				if (image.error == null)
					image.error = "TECH METADATA SAVING";
			}
		}
	}
	
	private void saveThumbnails(MediaTupleData mediaData, ArrayList<ImageInfo> imageInfos) {
		long start = System.currentTimeMillis();
		String repName = "thumbnail";
		String version;
		try {
			String uri = getRepresentationUri(mediaData, repName);
			UrlParser urlParser = new UrlParser(uri);
			version = urlParser.getPart(UrlPart.VERSIONS);
		} catch (DriverException | MCSException | MalformedURLException e) {
			logger.error("Could not store thumbnail representation in "
					+ mediaData.getEdmRepresentation().getCloudId(), e);
			for (ImageInfo image : imageInfos) {
				if (image.error == null)
					image.error = "THUMBNAIL SAVING";
			}
			return;
		}
		
		String cloudId = mediaData.getEdmRepresentation().getCloudId();
		for (ImageInfo image : imageInfos) {
			try {
				String md5 = DigestUtils.md5Hex(image.fileInfo.getUrl());
				String mimeType = "image/" + image.getThumbnailFormat();
				if (image.thumbnail200 != null) {
					fileClient.uploadFile(cloudId, repName, version, md5 + "-w200",
							new ByteArrayInputStream(image.thumbnail200), mimeType);
				}
				if (image.thumbnail400 != null) {
					fileClient.uploadFile(cloudId, repName, version, md5 + "-w400",
							new ByteArrayInputStream(image.thumbnail400), mimeType);
				}
			} catch (DriverException | MCSException e) {
				logger.error("Could not save thumbnails for cloudId " + cloudId, e);
				if (image.error == null)
					image.error = "THUMBNAIL SAVING";
			}
		}
		logger.debug("image saving took {} ms", System.currentTimeMillis() - start);
	}
	
	private String getRepresentationUri(MediaTupleData mediaData, String repName) throws MCSException {
		String cloudId = mediaData.getEdmRepresentation().getCloudId();
		String providerId = mediaData.getEdmRepresentation().getDataProvider();
		
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
		return uri.toString();
	}
	
	private String findImageMagickCommand() {
		boolean isWindows = System.getProperty("os.name").toLowerCase().contains("win");
		try {
			Process which = runCommand(Arrays.asList(isWindows ? "where" : "which", "magick"), true);
			List<String> paths = doAndClose(IOUtils::readLines, which.getInputStream());
			for (String path : paths) {
				Process test = runCommand(Arrays.asList(path, "-version"), true);
				String version = doAndClose(IOUtils::toString, test.getInputStream());
				if (version.matches("(?s)^Version: ImageMagick (6|7).*")) // (?s) means . matches newline
					return path;
			}
			throw new RuntimeException("ImageMagick command version 6/7 not found in " + paths);
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
	
	private static boolean isImage(String mimeType) {
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
	
	private static class ImageInfo {
		
		private static final Pattern MIME_TYPE = Pattern.compile("^  Mime type: (.*)", Pattern.MULTILINE);
		
		private static final Pattern GEOMETRY = Pattern.compile("^  Geometry: (\\d+)x(\\d+)", Pattern.MULTILINE);
		
		int width;
		int height;
		
		byte[] thumbnail200;
		byte[] thumbnail400;
		
		String error;
		
		FileInfo fileInfo;
		
		ImageInfo(FileInfo fileInfo) {
			this.fileInfo = fileInfo;
		}
		
		boolean shouldProcessMetadata() {
			return Arrays.asList(UrlType.HAS_VIEW, UrlType.IS_SHOWN_BY).stream()
					.anyMatch(fileInfo.getTypes()::contains);
		}
		
		boolean shouldProcessThumbnails() {
			return Arrays.asList(UrlType.OBJECT, UrlType.HAS_VIEW, UrlType.IS_SHOWN_BY).stream()
					.anyMatch(fileInfo.getTypes()::contains);
		}
		
		void extract(String identifyResult) throws MediaException {
			Matcher m = MIME_TYPE.matcher(identifyResult);
			Matcher g = GEOMETRY.matcher(identifyResult);
			if (m.find() && g.find()) {
				String mimeType = m.group(1);
				if (!mimeType.equals(fileInfo.getMimeType())) {
					throw new MediaException(
							"Mime type provided: " + fileInfo.getMimeType() + ", extracted: " + mimeType,
							"MIMETYPE INCONSISTENT");
				}
				width = Integer.parseInt(g.group(1));
				height = Integer.parseInt(g.group(2));
			} else {
				throw new MediaException("identify result missing data: " + identifyResult, "CORRUPTED");
			}
		}
		
		String getThumbnailFormat() {
			return Arrays.asList("application/pdf", "image/png").contains(fileInfo.getMimeType()) ? "png" : "jpeg";
		}
		
	}
	
	private class EdmWebResourceWrapper {
		final String ebucoreUri = "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#";
		private Document parent;
		private Element edmWebResource;
		
		EdmWebResourceWrapper(Document edm, String url) {
			this.parent = edm;
			edmWebResource = null;
			NodeList nList = edm.getElementsByTagName("edm:WebResource");
			for (int i = 0; i < nList.getLength(); i++) {
				Element node = (Element) nList.item(i);
				if (node.getAttributes().getNamedItem("rdf:about").getNodeValue().equals(url)) {
					edmWebResource = node;
					break;
				}
			}
			
			if (edmWebResource == null) {
				edmWebResource = edm.createElement("edm:WebResource");
				edmWebResource.setAttribute("rdf:about", url);
				edm.getDocumentElement().appendChild(edmWebResource);
			}
			parent.getDocumentElement().setAttribute("xmlns:ebucore", ebucoreUri);
		}
		
		void setData(ImageInfo image) {
			setValue("hasMimeType", image.fileInfo.getMimeType());
			setValue("fileByteSize", (long) image.fileInfo.getLength());
			setValue("width", image.width);
			setValue("height", image.height);
		}
		
		private void setValue(String name, Object value) {
			Element ebucoreElement = (Element) edmWebResource.getElementsByTagName(name).item(0);
			if (ebucoreElement == null) {
				ebucoreElement = parent.createElementNS(ebucoreUri, "ebucore:" + name);
				edmWebResource.appendChild(ebucoreElement);
				
				String type = value.getClass().getSimpleName();
				if (!"string".equalsIgnoreCase(type)) {
					ebucoreElement.setAttribute("rdf:datatype",
							"http://www.w3.org/2001/XMLSchema#" + StringUtils.lowerCase(type));
				}
			}
			ebucoreElement.setTextContent(String.valueOf(value));
		}
	}
	
}
