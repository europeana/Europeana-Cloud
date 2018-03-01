package eu.europeana.cloud.service.dps.storm.topologies.ic.topology.api;

import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.ConverterContext;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.ImageMagicJPGToTiff;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.KakaduConverterTiffToJP2;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.ConversionException;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.ICSException;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.UnexpectedExtensionsException;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis.ExtensionHelper;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.util.ImageConverterUtil;
import eu.europeana.cloud.service.dps.storm.utils.TaskTupleUtility;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.tika.mime.MimeTypeException;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by Tarek on 8/12/2015.
 */

/**
 * Implementation of image converter service
 */
public class ImageConverterServiceImpl implements ImageConverterService {

    private ConverterContext converterContext;
    private ImageConverterUtil imageConverterUtil;
    private final static Logger LOGGER = Logger.getLogger(ImageConverterServiceImpl.class);
    private static final String TIFF_EXTENSION = ".tiff";
    private static final String JPEG_EXTENSION = ".jpg";
    private static final String JPEG_MIME_TYPE = "image/jpeg";
    private static final String TIFF_MIME_TYPE = "image/tiff";


    public ImageConverterServiceImpl(ConverterContext converterContext, ImageConverterUtil imageConverterUtil) {
        this.converterContext = converterContext;
        this.imageConverterUtil = imageConverterUtil;
    }

    public ImageConverterServiceImpl() {
        converterContext = new ConverterContext(new ImageMagicJPGToTiff());
        imageConverterUtil = new ImageConverterUtil();
    }

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
        String fileUrl = stormTaskTuple.getFileUrl();
        LOGGER.info("The converting process for file " + fileUrl + " started successfully");
        String folderPath = null;

        try {
            ByteArrayInputStream inputStream = stormTaskTuple.getFileByteDataAsStream();
            if (inputStream != null) {
                String inputMimeType = TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.MIME_TYPE);
                if (!(TIFF_MIME_TYPE.equals(inputMimeType) || JPEG_MIME_TYPE.equals(inputMimeType)))
                    throw new AssertionError();
                String inputExtension = ExtensionHelper.getExtension(inputMimeType);
                String tempFileName = UUID.randomUUID().toString();
                folderPath = ImageConverterUtil.persistStreamToTemporaryStorage(inputStream, tempFileName, inputExtension);
                String inputFilePath = ImageConverterUtil.buildFilePath(folderPath, tempFileName, inputExtension);
                String outputExtension = ExtensionHelper.getExtension(TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.OUTPUT_MIME_TYPE));
                String outputFilePath = ImageConverterUtil.buildFilePath(folderPath, tempFileName, outputExtension);
                if (JPEG_EXTENSION.equals(inputExtension)) {
                    inputFilePath = convertJpgToTiff(folderPath, inputFilePath, tempFileName);
                }
                converterContext.setConverter(new KakaduConverterTiffToJP2());
                List<String> properties = new ArrayList<>();
                properties.add(TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.KAKADU_ARGUEMENTS));
                converterContext.convert(inputFilePath, outputFilePath, properties);
                stormTaskTuple.setFileData(imageConverterUtil.getStream(outputFilePath));
                final UrlParser urlParser = new UrlParser(fileUrl);
                if (urlParser.isUrlToRepresentationVersionFile()) {
                    stormTaskTuple.addParameter(PluginParameterKeys.CLOUD_ID, urlParser.getPart(UrlPart.RECORDS));
                    stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_NAME, urlParser.getPart(UrlPart.REPRESENTATIONS));
                    stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_VERSION, urlParser.getPart(UrlPart.VERSIONS));
                }
                stormTaskTuple.addParameter(PluginParameterKeys.OUTPUT_FILE_NAME, tempFileName + outputExtension);
                stormTaskTuple.getParameters().remove(PluginParameterKeys.MIME_TYPE);
                LOGGER.info("The converting process for file " + fileUrl + " completed successfully");
            }
        } finally {
            if (folderPath != null)
                FileUtils.deleteDirectory(new File(folderPath));

        }
    }

    private String convertJpgToTiff(String folderPath, String inputFilePath, String outputFileName) throws UnexpectedExtensionsException, ConversionException, IOException {
        String outputFilePath = ImageConverterUtil.buildFilePath(folderPath, outputFileName, TIFF_EXTENSION);
        converterContext.convert(inputFilePath, outputFilePath, null);
        return outputFilePath;
    }

}
