package eu.europeana.cloud.service.dps.storm.topologies.ic.topology.api;


import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.ConverterContext;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.KakaduConverterTiffToJP2;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.ICSException;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis.ExtensionHelper;
import eu.europeana.cloud.service.dps.storm.utils.TaskTupleUtility;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.tika.mime.MimeTypeException;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Tarek on 8/12/2015.
 */

/**
 * Implementation of image converter service
 */
public class ImageConverterServiceImpl implements ImageConverterService {

    private ConverterContext converterContext;
    private final static Logger LOGGER = Logger.getLogger(ImageConverterServiceImpl.class);

    private static final int BATCH_MAX_SIZE = 1240 * 4;

    /**
     * Converts image file with a format to the same image with different format
     *
     * @param stormTaskTuple Tuple which DpsTask is part of ...
     * @return path for the newly created file
     * @throws MCSException      on unexpected situations.
     * @throws ICSException
     * @throws MimeTypeException : when mimetype is not recognized or null
     * @throws IOException
     */
    @Override
    public void convertFile(StormTaskTuple stormTaskTuple) throws IOException, MimeTypeException, MCSException, ICSException, RuntimeException {
        converterContext = new ConverterContext(new KakaduConverterTiffToJP2());
        LOGGER.info("The converting process for file " + stormTaskTuple.getFileUrl() + " started successfully");
        String folderPath = null;

        try {
            ByteArrayInputStream inputStream = stormTaskTuple.getFileByteDataAsStream();
            if (inputStream != null) {
                Map<String, String> urlParams = FileServiceClient.parseFileUri(stormTaskTuple.getFileUrl());
                String fileName = urlParams.get(ParamConstants.P_FILENAME);
                String cleanName = FilenameUtils.removeExtension(fileName);
                String inputExtension = ExtensionHelper.getExtension(TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.MIME_TYPE));
                long randomValue = UUID.randomUUID().getMostSignificantBits();
                String tempFileName = String.valueOf(randomValue);
                folderPath = persistStreamToTemporaryStorage(inputStream, tempFileName, inputExtension);
                String inputFilePath = buildFilePath(folderPath, tempFileName, inputExtension);
                String outputExtension = ExtensionHelper.getExtension(TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.OUTPUT_MIME_TYPE));
                String outputFilePath = buildFilePath(folderPath, tempFileName, outputExtension);
                if (outputFilePath != null) {
                    List<String> properties = new ArrayList<>();
                    properties.add(TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.KAKADU_ARGUEMENTS));
                    converterContext.convert(inputFilePath, outputFilePath, properties);
                    File outputFile = new File(outputFilePath);
                    InputStream outputStream = new FileInputStream(outputFile);
                    stormTaskTuple.setFileData(outputStream);
                    stormTaskTuple.addParameter(PluginParameterKeys.OUTPUT_FILE_NAME, cleanName + outputExtension);
                    LOGGER.info("The converting process for file " + stormTaskTuple.getFileUrl() + " completed successfully");
                }
            }
        } finally {
            FileUtils.deleteDirectory(new java.io.File(folderPath));
        }
    }

    private String buildFilePath(String folderPath, String fileName, String extension) {
        return folderPath + fileName + "." + extension;
    }

    private String persistStreamToTemporaryStorage(ByteArrayInputStream inputStream, String fileName, String extension) throws IOException, RuntimeException {
        OutputStream outputStream = null;
        String folderPath = null;
        try {
            folderPath = Files.createTempDirectory(fileName) + File.separator;
            File file = new File(folderPath + fileName + "." + extension);
            outputStream = new FileOutputStream(file.toPath().toString());
            byte[] buffer = new byte[BATCH_MAX_SIZE];
            IOUtils.copyLarge(inputStream, outputStream, buffer);
        } finally {
            if (outputStream != null)
                outputStream.close();
            inputStream.close();
        }
        return folderPath;
    }

}
