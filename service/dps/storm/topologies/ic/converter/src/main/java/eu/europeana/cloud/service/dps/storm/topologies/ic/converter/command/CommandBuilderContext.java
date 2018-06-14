package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.command;

import java.util.List;

/**
 * Context for choosing the appropriate Command Builder
 */
public class CommandBuilderContext {
    private CommandBuilder commandBuilder;

    public CommandBuilderContext(CommandBuilder commandBuilder) {
        this.commandBuilder = commandBuilder;
    }

    /**
     * Build full-fledged command by calling the correct method based on the builder context
     *
     * @param inputFilePath  The input file full path
     * @param outputFilePath The output file full path
     * @param properties     List of properties attached to the kakadu command
     * @return Full-fledged command .
     */
    public String constructCommand(String inputFilePath,
                                   String outputFilePath, List<String> properties) {

        return commandBuilder.constructCommand(inputFilePath, outputFilePath,
                properties);

    }

}
