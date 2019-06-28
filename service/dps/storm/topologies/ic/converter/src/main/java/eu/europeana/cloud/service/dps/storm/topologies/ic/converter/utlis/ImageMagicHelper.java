package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis;

import java.util.List;


/**
 * Utility for image magic.
 */
public class ImageMagicHelper {

    /**
     * Build full image magic shell command based on input parameters
     *
     * @param imageMagicConsoleCommand image magic shell command
     * @param inputFilePath            The input file full path
     * @param outputFilePath           The output file full path
     * @param properties               List of properties attached to the image magic command
     * @return the full-fledged image magic command .
     */
    public static String constructCommand(String imageMagicConsoleCommand, String inputFilePath,
                                          String outputFilePath, List<String> properties) {

        if (inputFilePath != null && outputFilePath != null) {
            StringBuffer command = new StringBuffer();
            command.append(imageMagicConsoleCommand);
            command.append(' ');
            command.append(inputFilePath);
            command.append(' ');
            command.append(outputFilePath);
            if (properties != null) {
                for (String property : properties) {
                    command.append(" " + property);
                }
            }
            return command.toString();
        }
        return null;

    }

}