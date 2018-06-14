package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.command;

import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis.KakaduHelper;

import java.util.List;

/**
 * Utility for building a full-fledged kakadu compress shell command based on input parameters
 */
public class KakaduCompressCommandBuilder implements CommandBuilder {
    private static final String KAKADU_COMPRESS_COMMAND = "kdu_compress";

    /**
     * Build  kakadu compress shell command based on input parameters
     *
     * @param inputFilePath  The input file full path
     * @param outputFilePath The output file full path
     * @param properties     List of properties attached to the kakadu command
     * @return Kakadu compress shell command .
     */
    public String constructCommand(String inputFilePath,
                                   String outputFilePath, List<String> properties) {
        return KakaduHelper.constructCommand(KAKADU_COMPRESS_COMMAND, inputFilePath,
                outputFilePath, properties);
    }

}
