package eu.europeana.cloud.http.bolt;

/**
 * Created by Tarek on 3/19/2018.
 */

import com.rits.cloning.Cloner;
import eu.europeana.cloud.http.common.CompressionFileExtension;
import eu.europeana.cloud.http.common.UnpackingServiceFactory;
import eu.europeana.cloud.http.service.FileUnpackingService;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;


public class HTTPHarvesterBolt extends AbstractDpsBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(HTTPHarvesterBolt.class);
    private static final int BATCH_MAX_SIZE = 1240 * 4;
    public static final String CLOUD_SEPARATOR = "_";
    public static final String MAC_TEMP_FOLDER = "__MACOSX";
    public static final String MAC_TEMP_FILE = ".DS_Store";


    public void execute(StormTaskTuple stormTaskTuple) {
        File file = null;
        try {
            String httpURL = stormTaskTuple.getParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA);
            file = downloadFile(httpURL);
            String compressingExtension = FilenameUtils.getExtension(file.getName());
            FileUnpackingService fileUnpackingService = UnpackingServiceFactory.createUnpackingService(compressingExtension);
            fileUnpackingService.unpackFile(file.getAbsolutePath(), file.getParent() + File.separator);
            Path start = Paths.get(new File(file.getParent()).toURI());
            emitFiles(start, stormTaskTuple);
        } catch (Exception e) {
            LOGGER.error("HTTPHarvesterBolt error: {} ", e.getMessage());
            logAndEmitError(stormTaskTuple, e.getMessage());
        } finally {
            removeTempFolder(file);
        }
    }


    private File downloadFile(String httpURL) throws IOException {
        URL url = new URL(httpURL);
        URLConnection conn = url.openConnection();
        InputStream inputStream = conn.getInputStream();
        OutputStream outputStream = null;
        try {
            String tempFileName = UUID.randomUUID().toString();
            String folderPath = Files.createTempDirectory(tempFileName) + File.separator;
            File file = new File(folderPath + FilenameUtils.getName(httpURL));
            outputStream = new FileOutputStream(file.toPath().toString());
            byte[] buffer = new byte[BATCH_MAX_SIZE];
            IOUtils.copyLarge(inputStream, outputStream, buffer);
            return file;
        } finally {
            if (outputStream != null)
                outputStream.close();
            if (inputStream != null)
                inputStream.close();
        }
    }

    private void emitFiles(final Path start, final StormTaskTuple stormTaskTuple) throws IOException {
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                String fileName = getFileNameFromPath(file);
                if (fileName.equals(MAC_TEMP_FILE))
                    return FileVisitResult.CONTINUE;
                String extension = FilenameUtils.getExtension(file.toString());
                if (!CompressionFileExtension.contains(extension)) {
                    String mimeType = Files.probeContentType(file);
                    String filePath = file.toString();
                    String readableFileName = filePath.substring(start.toString().length() + 1).replaceAll("\\\\", "/");
                    emitFileContent(stormTaskTuple, filePath, readableFileName, mimeType);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                String dirName = getFileNameFromPath(dir);
                if (dirName.equals(MAC_TEMP_FOLDER))
                    return FileVisitResult.SKIP_SUBTREE;
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private String getFileNameFromPath(Path path) {
        if (path != null)
            return path.getFileName().toString();
        throw new IllegalArgumentException("Path parameter should never be null");
    }

    private void emitFileContent(StormTaskTuple stormTaskTuple, String filePath, String readableFilePath, String mimeType) throws IOException {
        FileInputStream fileInputStream = null;
        try {
            StormTaskTuple tuple = new Cloner().deepClone(stormTaskTuple);
            File file = new File(filePath);
            fileInputStream = new FileInputStream(file);
            tuple.setFileData(fileInputStream);
            tuple.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, mimeType);
            String localId = formulateLocalId(readableFilePath);
            tuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, localId);
            tuple.setFileUrl(readableFilePath);
            outputCollector.emit(inputTuple, tuple.toStormTuple());
        } finally {
            if (fileInputStream != null)
                fileInputStream.close();
        }
    }

    private String formulateLocalId(String readableFilePath) {
        return new StringBuilder(readableFilePath).append(CLOUD_SEPARATOR).append(UUID.randomUUID().toString()).toString();
    }

    private void removeTempFolder(File file) {
        if (file != null)
            try {
                FileUtils.deleteDirectory(new File(file.getParent()));
            } catch (IOException e) {
                LOGGER.error("ERROR while removing the temp Folder", e.getMessage());
            }
    }

    @Override
    public void prepare() {
    }
}

