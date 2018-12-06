package eu.europeana.cloud.dps.topologies.media.support;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.MTDSerializer;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.metis.mediaprocessing.RdfConverter.Parser;
import eu.europeana.metis.mediaprocessing.RdfConverter.Writer;
import eu.europeana.metis.mediaprocessing.exception.RdfConverterException;
import eu.europeana.metis.mediaprocessing.exception.RdfDeserializationException;
import eu.europeana.metis.mediaprocessing.exception.RdfSerializationException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import eu.europeana.metis.mediaprocessing.model.UrlType;
import eu.europeana.metis.mediaprocessing.temp.DownloadedResource;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@DefaultSerializer(MTDSerializer.class)
public class MediaTupleData {

    @DefaultSerializer(JavaSerializer.class)
    public static class FileInfo implements Serializable, DownloadedResource, RdfResourceEntry {

        private final String url;
        private File content;
        private String mimeType;
        private InetAddress contentSource;
        private boolean errorFlag = false;
        private Set<UrlType> urlTypes;

        public FileInfo(RdfResourceEntry resourceEntry) {
            this.url = resourceEntry.getResourceUrl();
            this.urlTypes = resourceEntry.getUrlTypes();
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

        public void setErrorFlag(Boolean errorFlag) {
            this.errorFlag = errorFlag;
        }

        public boolean isErrorFlagSet() {
            return this.errorFlag;
        }

        @Override
        public Set<UrlType> getUrlTypes() {
            return Collections.unmodifiableSet(this.urlTypes);
        }

        @Override
        public String getResourceUrl() {
            return getUrl();
        }

        @Override
        public Path getContentPath() {
            return getContent() != null ? getContent().toPath() : null;
        }
    }

    public static final String FIELD_NAME = "mediaTopology.mediaData";

    final private long taskId;
    final private Representation edmRepresentation;

    private RDF edm;
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

    public RDF getEdm() {
        return edm;
    }

    public void setEdm(RDF edm) {
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

        private final Parser deserializer;
        private final Writer serializer;

        public MTDSerializer() {
            try {
                deserializer = new Parser();
                serializer = new Writer();
            } catch (RdfConverterException e) {
                throw new RuntimeException("EDM serializer construction failed", e);
            }
        }

        @Override
        public void write(Kryo kryo, Output output, MediaTupleData data) {
            output.writeLong(data.taskId);
            kryo.writeObject(output, data.edmRepresentation);
            try {
                kryo.writeObject(output, serializer.serialize(data.edm));
            } catch (RdfSerializationException e) {
                throw new RuntimeException("EDM writing failed", e);
            }
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
                data.edm = deserializer.deserialize(byteStream);
            } catch (RdfDeserializationException | IOException e) {
                throw new RuntimeException("EDM parsing failed", e);
            }
            data.fileInfos = kryo.readObject(input, ArrayList.class);
            data.connectionLimitsPerSource = kryo.readObject(input, HashMap.class);
            data.task = kryo.readObject(input, DpsTask.class);

            return data;
        }

    }
}
