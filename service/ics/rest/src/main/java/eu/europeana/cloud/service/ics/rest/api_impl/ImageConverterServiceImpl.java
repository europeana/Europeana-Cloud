package eu.europeana.cloud.service.ics.rest.api_impl;


import static eu.europeana.cloud.common.web.ParamConstants.VERSIONS;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.ics.converter.converter.ConverterContext;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.ics.converter.exceptions.ICSException;
import eu.europeana.cloud.service.ics.converter.exceptions.UnexpectedExtensionsException;
import eu.europeana.cloud.service.ics.converter.utlis.MimeTypeHelper;
import eu.europeana.cloud.service.ics.rest.api.ImageConverterService;
import eu.europeana.cloud.service.ics.rest.data.FileInputParameter;
import eu.europeana.cloud.service.ics.converter.utlis.ExtensionHelper;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.tika.mime.MimeTypeException;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;

/**
 * Created by Tarek on 8/12/2015.
 */

/**
 * Implementation of image converter service
 */
public class ImageConverterServiceImpl implements ImageConverterService {
    @Autowired
    private FileServiceClient fileServiceClient;

    @Autowired
    private RecordServiceClient recordServiceClient;

    @Autowired
    private ConverterContext converterContext;

    private final static Logger LOGGER = Logger.getLogger(ImageConverterServiceImpl.class);


    @Override

    /**
     *  gets The image from ecloud converts it based on the choosen converter context and upload the newly created file into ecloud again with new representation.
     *
     @param fileInputParameter The input parameter which should have variables needed to specify the input/output file and the properties of the conversion
      * @return url for the newly created file
     * @throws RepresentationNotExistsException when representation does not exist in specified version.
     * @throws FileNotExistsException           when requested file does not exist.
     * @throws DriverException                  call to service has not succeeded because of server side error.
     * @throws MCSException                     on unexpected situations.
     * @throws ICSException                     on unexpected situations.
     * @throws IOException
     */
    public String convertFile(FileInputParameter fileInputParameter) throws RepresentationNotExistsException, FileNotExistsException, UnexpectedExtensionsException, IOException, DriverException, MCSException, ICSException {
        LOGGER.info("The converting process for file " + fileInputParameter.getFileName() + "started successfully");

        String inputFilePath = null;
        String outputFilePath = null;
        String folderPath = null;
        URI convertedFileURL = null;
        InputStream inputStream = null;
        try {
            inputStream = fileServiceClient.getFile(fileInputParameter.getCloudId(), fileInputParameter.getInputRepresentationName(), fileInputParameter.getInputVersion(), fileInputParameter.getFileName());
            if (inputStream != null) {
                folderPath = persistStreamToTemporaryStorage(inputStream, fileInputParameter.getFileName());
                inputFilePath = buildFilePath(folderPath, fileInputParameter.getFileName(), fileInputParameter.getInputExtension());
                outputFilePath = buildFilePath(folderPath, fileInputParameter.getFileName(), fileInputParameter.getOutputExtension());
                converterContext.convert(inputFilePath, outputFilePath, fileInputParameter.getProperties());
            }

            URI uri = recordServiceClient.createRepresentation(fileInputParameter.getCloudId(), fileInputParameter.getOutputRepresentationName(), fileInputParameter.getProviderId());
            if (uri != null) {
                Representation representation = recordServiceClient.getRepresentation(fileInputParameter.getCloudId(), fileInputParameter.getOutputRepresentationName(), getVersionFromUri(uri));
                if (outputFilePath != null) {
                    java.io.File outputFile = new java.io.File(outputFilePath);
                    InputStream is = new FileInputStream(outputFile);
                    String outputMimeType = MimeTypeHelper.getMimeTypeFromExtension(fileInputParameter.getOutputExtension());
                    convertedFileURL = fileServiceClient.uploadFile(fileInputParameter.getCloudId(), fileInputParameter.getOutputRepresentationName(), representation.getVersion(), is, outputMimeType);
                    LOGGER.info("The converting process for file " + fileInputParameter.getFileName() + "completed successfully");

                }

            }

        } catch (MimeTypeException e) {
            throw new ICSException(e);

        } finally {
            FileUtils.deleteDirectory(new java.io.File(folderPath));
            inputStream.close();

        }
        return convertedFileURL.toString();
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
            folderPath = Files.createTempDirectory(fileName) + java.io.File.separator;
            java.io.File file = new java.io.File(folderPath + fileName + inputFileExtension);
            outputStream = new FileOutputStream(file.toPath().toString());
            baos.writeTo(outputStream);
        } finally {
            outputStream.close();
        }


        return folderPath;
    }

    private String getVersionFromUri(URI uri) {
        String url = uri.toString();
        String version = null;
        if (url.contains(VERSIONS)) {
            version = url.substring(url.lastIndexOf(VERSIONS) + VERSIONS.length() + 1, url.length());

        }
        return version;
    }

}
