package eu.europeana.cloud.service.dps.storm.topologies.ic.topology.api;


import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.ConverterContext;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.KakaduConverterTiffToJP2;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.ICSException;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis.ExtensionHelper;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis.MimeTypeHelper;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import eu.europeana.cloud.service.dps.storm.utils.TaskTupleUtility;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.tika.mime.MimeTypeException;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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


    /**
     * Converts image file with a format to the same image with different format
     *
     * @param stormTaskTuple Tuple which DpsTask is part of ...
     * @return path for the newly created file
     * @throws MCSException on unexpected situations.
     * @throws ICSException
     * @throws IOException
     */
    @Override
    public void convertFile(StormTaskTuple stormTaskTuple) throws IOException, MCSException, ICSException {
        converterContext = new ConverterContext(new KakaduConverterTiffToJP2());
        URI fileURI = URI.create(stormTaskTuple.getFileUrl());

        LOGGER.info("The converting process for file " + stormTaskTuple.getFileUrl() + " started successfully");

        String folderPath = null;
        InputStream inputStream = null;
        InputStream outputStream = null;
        try {
            TaskTupleUtility taskTupleUtility = new TaskTupleUtility();
            inputStream = stormTaskTuple.getFileByteDataAsStream();
            if (inputStream != null) {
                String fileName = findFileName(fileURI);
                folderPath = persistStreamToTemporaryStorage(inputStream, fileName);
                String inputFilePath = buildFilePath(folderPath, fileName, taskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.INPUT_EXTENSION));
                String outputFilePath = buildFilePath(folderPath, fileName, taskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.OUTPUT_EXTENSION));
                if (outputFilePath != null) {
                    List<String> properties = new ArrayList<>();
                    properties.add(taskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.KAKADU_ARGUEMENTS));
                    converterContext.convert(inputFilePath, outputFilePath, properties);
                    File outputFile = new File(outputFilePath);
                    outputStream = new FileInputStream(outputFile);
                    stormTaskTuple.setFileData(outputStream);
                    LOGGER.info("The converting process for file " + stormTaskTuple.getFileUrl() + " completed successfully");
                }

            }


        } catch (MimeTypeException e) {
            throw new ICSException(e);

        } finally {
            outputStream.close();
            FileUtils.deleteDirectory(new java.io.File(folderPath));
            inputStream.close();


        }

    }


    private String buildFilePath(String folderPath, String fileName, String extension) {
        return new StringBuilder(folderPath).append(fileName).append(".").append(extension).toString();
    }

    private String persistStreamToTemporaryStorage(InputStream inputStream, String fileName) throws MimeTypeException, IOException {
        OutputStream outputStream = null;
        String folderPath = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IOUtils.copy(inputStream, baos);
            byte[] bytes = baos.toByteArray();
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            String inputFileMimeType = MimeTypeHelper.getMimeTypeFromStream(bais);
            String inputFileExtension = ExtensionHelper.getExtension(inputFileMimeType);
            folderPath = Files.createTempDirectory(fileName) + File.separator;
            File file = new File(folderPath + fileName + inputFileExtension);
            outputStream = new FileOutputStream(file.toPath().toString());
            baos.writeTo(outputStream);
        } finally {
            outputStream.close();
        }


        return folderPath;
    }

    private String findFileName(URI uri) throws MCSException {
        Pattern p = Pattern.compile(".*/records/([^/]+)/representations/([^/]+)/versions/([^/]+)/files/([^/]+)");
        Matcher m = p.matcher(uri.toString());

        if (m.find()) {
            return m.group(4);
        } else {
            throw new MCSException("Unable to find file in representation URL");
        }
    }


}
