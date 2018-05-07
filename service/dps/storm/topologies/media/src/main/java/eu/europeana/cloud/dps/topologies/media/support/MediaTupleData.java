package eu.europeana.cloud.dps.topologies.media.support;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.MTDSerializer;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.metis.mediaservice.EdmObject;
import eu.europeana.metis.mediaservice.MediaException;

@DefaultSerializer(MTDSerializer.class)
public class MediaTupleData {
	
	@DefaultSerializer(JavaSerializer.class)
	public static class FileInfo implements Serializable {
		private final String url;
		private File content;
		private String mimeType;
		private InetAddress contentSource;
		
		public FileInfo(String url) {
			this.url = url;
		}
		
		public File getContent() {
			return content;
		}
		
		public void setContent(File content) {
			this.content = content;
		}
		
		public InetAddress getContentSource() {
			return contentSource;
		}
		
		public void setContentSource(InetAddress contentSource) {
			this.contentSource = contentSource;
		}
		
		public String getMimeType() {
			return mimeType;
		}
		
		public void setMimeType(String mimeType) {
			this.mimeType = mimeType;
		}
		
		public String getUrl() {
			return url;
		}
	}
	
	public static final String FIELD_NAME = "mediaTopology.mediaData";
	
	final private long taskId;
	final private Representation edmRepresentation;
	
	private EdmObject edm;
	private List<FileInfo> fileInfos = Collections.emptyList();
	private Map<String, Integer> connectionLimitsPerSource = Collections.emptyMap();
	
	private DpsTask task;
	
	public MediaTupleData(long taskId, Representation edmRepresentation) {
		this.taskId = taskId;
		this.edmRepresentation = edmRepresentation;
	}
	
	public long getTaskId() {
		return taskId;
	}
	
	public Representation getEdmRepresentation() {
		return edmRepresentation;
	}
	
	public List<FileInfo> getFileInfos() {
		return fileInfos;
	}
	
	public void setFileInfos(List<FileInfo> fileInfos) {
		this.fileInfos = fileInfos;
	}
	
	public EdmObject getEdm() {
		return edm;
	}
	
	public void setEdm(EdmObject edm) {
		this.edm = edm;
	}
	
	public Map<String, Integer> getConnectionLimitsPerSource() {
		return connectionLimitsPerSource;
	}
	
	public void setConnectionLimitsPerSource(Map<String, Integer> connectionLimitsPerSource) {
		this.connectionLimitsPerSource = connectionLimitsPerSource;
	}
	
	public DpsTask getTask() {
		return task;
	}
	
	public void setTask(DpsTask task) {
		this.task = task;
	}
	
	public static class MTDSerializer extends com.esotericsoftware.kryo.Serializer<MediaTupleData> {
		
		EdmObject.Parser parser = new EdmObject.Parser();
		EdmObject.Writer writer = new EdmObject.Writer();
		
		@Override
		public void write(Kryo kryo, Output output, MediaTupleData data) {
			output.writeLong(data.taskId);
			kryo.writeObject(output, data.edmRepresentation);
			kryo.writeObject(output, writer.toXmlBytes(data.edm));
			kryo.writeObject(output, new ArrayList<>(data.fileInfos));
			kryo.writeObject(output, new HashMap<>(data.connectionLimitsPerSource));
			kryo.writeObject(output, data.task);
		}
		
		@Override
		public MediaTupleData read(Kryo kryo, Input input, Class<MediaTupleData> type) {
			long taskId = input.readLong();
			Representation representation = kryo.readObject(input, Representation.class);
			MediaTupleData data = new MediaTupleData(taskId, representation);
			try (ByteArrayInputStream byteStream = new ByteArrayInputStream(kryo.readObject(input, byte[].class))) {
				data.edm = parser.parseXml(byteStream);
			} catch (MediaException | IOException e) {
				throw new RuntimeException("EDM parsing failed", e);
			}
			data.fileInfos = kryo.readObject(input, ArrayList.class);
			data.connectionLimitsPerSource = kryo.readObject(input, HashMap.class);
			data.task = kryo.readObject(input, DpsTask.class);
			
			return data;
		}
		
	}
}
