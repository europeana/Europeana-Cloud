package eu.europeana.cloud.dps.topologies.media;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.dps.topologies.media.support.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.MediaException;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.UrlType;
import eu.europeana.cloud.dps.topologies.media.support.Util;
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
	
	private String magickCmd;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		magickCmd = findImageMagickCommand();
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
		for (Entry<UrlType, List<String>> entry : mediaData.getFileUrls().entrySet()) {
			for (String url : entry.getValue()) {
				
				if (!processedUrls.add(url))
					continue;
				try {
					ImageInfo imageInfo = new ImageInfo(mediaData.getFileInfos().get(url));
					if (imageInfo.shouldBeMetadataExtracted()) {
						String identifyResult = getIdentifyResult(imageInfo.fileInfo.getContent());
						imageInfo.prepareParameters(identifyResult);
						createTechnicalMetadata(mediaData, imageInfo);
						createThumbnails(mediaData, imageInfo);
					}
				} catch (IOException e) {
					logger.error("Image Magick identify: I/O error on " + edmRep, e);
				} catch (MediaException e) {
					logger.error("Processing url " + url + " failed", e);
				}
			}
		}
	}
	
	private String getIdentifyResult(byte[] content) throws IOException {
		Process identifyProcess = runCommand(Arrays.asList(magickCmd, "identify", "-verbose", "-"), false, content);
		return doAndClose(IOUtils::toString, identifyProcess.getInputStream());
	}
	
	private void createThumbnails(MediaTupleData mediaData, ImageInfo imageInfo)
			throws IOException, MediaException {
		int width = imageInfo.width;
		if (width < 200) {
			createImageThumbnail(imageInfo, mediaData, width);
		} else {
			createImageThumbnail(imageInfo, mediaData, 200);
		}
		if (width < 400 && width >= 200) {
			createImageThumbnail(imageInfo, mediaData, width);
		} else {
			createImageThumbnail(imageInfo, mediaData, 400);
		}
	}
	
	private void createImageThumbnail(ImageInfo info, MediaTupleData mediaData, int width)
			throws IOException, MediaException {
		
		Process convertProcess = runCommand(prepareConvertParams(info, width), false, info.fileInfo.getContent());
		String name = createThumbnailName(info.fileInfo.getUrl(), width);
		Representation edmRep = mediaData.getEdmRepresentation();
		storeRepresentation(edmRep.getCloudId(), edmRep.getDataProvider(), name,
				IOUtils.toByteArray(convertProcess.getInputStream()), info.getMimeTypeOfMiniature());
	}
	
	private void createTechnicalMetadata(MediaTupleData mediaData, ImageInfo imageInfo)
			throws MediaException {
		Representation edmRep = mediaData.getEdmRepresentation();
		
		EdmWebResourceWrapper edmWebResource =
				new EdmWebResourceWrapper(mediaData.getEdm(), imageInfo.fileInfo.getUrl());
		
		edmWebResource.setValue("ebucore:hasMimeType", imageInfo.mimeType);
		edmWebResource.setValue("ebucore:fileByteSize", imageInfo.fileInfo.getLength());
		edmWebResource.setValue("ebucore:width", imageInfo.width);
		edmWebResource.setValue("ebucore:height", imageInfo.height);
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try {
			TransformerFactory.newInstance().newTransformer().transform(new DOMSource(edmWebResource.parent),
					new StreamResult(outputStream));
		} catch (TransformerException | TransformerFactoryConfigurationError e) {
			throw new MediaException(
					"Could not transform xml document of the representation " + edmRep.getCloudId()
							+ "/techmetadata",
					e);
		}
		storeRepresentation(edmRep.getCloudId(), edmRep.getDataProvider(), "techmetadata",
				outputStream.toByteArray(), "text/xml");
		
	}
	
	private List<String> prepareConvertParams(ImageInfo info, int width) {
		String originalMimeType = info.mimeType;
		List<String> params = Arrays.asList();
		
		if (originalMimeType.equals("application/pdf")) {
			params = Arrays.asList(magickCmd, "convert", "-", "-background", "white", "-alpha", "remove", "-thumbnail",
					width + "x",
					"png:-");
		} else if (originalMimeType.equals("image/png")) {
			params = Arrays.asList(magickCmd, "convert", "-", "-thumbnail", width + "x", "png:-");
		} else {
			params = Arrays.asList(magickCmd, "convert", "-", "-thumbnail", width + "x", "-");
		}
		return params;
	}
	
	private String createThumbnailName(String url, int width) throws MediaException {
		String name = "";
		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			name = (new HexBinaryAdapter()).marshal(md5.digest(url.getBytes())) + "-w" + width;
		} catch (NoSuchAlgorithmException e) {
			throw new MediaException(e.getMessage(), e);
		}
		return name;
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
	
	private void storeRepresentation(String cloudId, String providerId, String repName, byte[] data,
			String mimeType)
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
	
	private static class ImageInfo {
		
		private static final Pattern MIME_TYPE = Pattern.compile("^  Mime type: (.*)", Pattern.MULTILINE);
		
		private static final Pattern GEOMETRY = Pattern.compile("^  Geometry: (\\d+)x(\\d+)", Pattern.MULTILINE);
		
		public String mimeType = "";
		public int width;
		public int height;
		
		private FileInfo fileInfo;
		
		public ImageInfo(FileInfo fileInfo) {
			this.fileInfo = fileInfo;
		}
		
		public boolean shouldBeMetadataExtracted() {
			String fMimeType = fileInfo.getMimeType();
			return (fMimeType.equals("application/pdf") || fMimeType.startsWith("image"))
					&& !Collections.disjoint(Arrays.asList(UrlType.OBJECT, UrlType.HAS_VIEW, UrlType.IS_SHOWN_BY),
							fileInfo.getTypes());
		}
		
		public void prepareParameters(String identifyResult) {
			Matcher m = MIME_TYPE.matcher(identifyResult);
			if (m.find())
				mimeType = m.group(1);
			m = GEOMETRY.matcher(identifyResult);
			if (m.find()) {
				width = Integer.parseInt(m.group(1));
				height = Integer.parseInt(m.group(2));
			}
		}
		
		public String getMimeTypeOfMiniature() {
			return Arrays.asList("application/pdf", "image/png").contains(mimeType) ? "image/png" : "image/jpeg";
		}
		
	}
	
	private class EdmWebResourceWrapper {
		private Document parent;
		private Element edmWebResource;
		
		public EdmWebResourceWrapper(Document edm, String url) {
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
			
		}
		
		public void setValue(String name, Object value) {
			Element ebucoreElement =
					(Element) edmWebResource.getElementsByTagName(name).item(0);
			if (ebucoreElement == null) {
				ebucoreElement = parent.createElementNS(
						"http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#", name);
				edmWebResource.appendChild(ebucoreElement);
				
				String type = value.getClass().getSimpleName();
				if (!type.equalsIgnoreCase("string")) {
					ebucoreElement.setAttribute("rdf:datatype",
							"http://www.w3.org/2001/XMLSchema#" + StringUtils.lowerCase(type));
				}
			}
			ebucoreElement.setTextContent(String.valueOf(value));
		}
	}
	
}
