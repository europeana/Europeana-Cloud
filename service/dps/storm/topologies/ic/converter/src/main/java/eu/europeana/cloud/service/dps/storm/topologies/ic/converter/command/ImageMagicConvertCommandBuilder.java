package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.command;

import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis.ImageMagicHelper;

import java.util.List;

/**
 * Utility for building a full-fledged image magic convert shell command based on input parameters
 */
public class ImageMagicConvertCommandBuilder implements CommandBuilder {
    private static final String IMAGE_MAGIC_CONVERT_COMMAND = "magick";

    /**
     * Build  kakadu compress shell command based on input parameters
     *
     * @param inputFilePath  The input file full path
     * @param outputFilePath The output file full path
     * @param properties     List of properties attached to the kakadu command
     * @return image magic convert shell command .
     */
    public String constructCommand(String inputFilePath,
                                   String outputFilePath, List<String> properties) {
        return ImageMagicHelper.constructCommand(IMAGE_MAGIC_CONVERT_COMMAND, inputFilePath,
                outputFilePath, properties);
    }

}
