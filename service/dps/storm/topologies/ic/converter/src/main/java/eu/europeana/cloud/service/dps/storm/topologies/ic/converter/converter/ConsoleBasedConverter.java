package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter;


import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.command.CommandBuilderContext;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.ConversionException;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.UnexpectedExtensionsException;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.extension.ExtensionCheckerContext;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis.CommandExecutor;

import java.io.IOException;
import java.util.List;

/**
 * Service for converting one file to another by executing shell command
 */
public class ConsoleBasedConverter implements Converter {
    private CommandBuilderContext commandBuilderContext;
    private ExtensionCheckerContext inputFileExtensionChecker;
    private ExtensionCheckerContext outputFileExtensionChecker;

    /**
     * Constructs a ConsoleBasedConverter with the specified command context and extension checkers contexts.
     *
     * @param commandBuilderContext      Command builder context
     * @param inputFileExtensionChecker  Extension checker context for input file
     * @param outputFileExtensionChecker Extension checker context for output file
     */
    public ConsoleBasedConverter(CommandBuilderContext commandBuilderContext,
                                 ExtensionCheckerContext inputFileExtensionChecker,
                                 ExtensionCheckerContext outputFileExtensionChecker) {
        this.commandBuilderContext = commandBuilderContext;
        this.inputFileExtensionChecker = inputFileExtensionChecker;
        this.outputFileExtensionChecker = outputFileExtensionChecker;

    }

    /**
     * Converts File to another by executing a shell command and list of properties
     *
     * @param inputFilePath  The input file full path
     * @param outputFilePath The output file full path
     * @param properties     List of properties attached to the kakadu command
     * @throws UnexpectedExtensionsException : throws when providing invalid or inconsistent extensions
     * @throws IOException
     */
    public void convert(String inputFilePath, String outputFilePath,
                        List<String> properties) throws UnexpectedExtensionsException, ConversionException, IOException {

        if (inputFileExtensionChecker.isGoodExtension(inputFilePath) && (outputFileExtensionChecker.isGoodExtension(outputFilePath))) {
            String command = commandBuilderContext.constructCommand(
                    inputFilePath, outputFilePath, properties);
            CommandExecutor commandExecutor = new CommandExecutor();
            commandExecutor.execute(command);
        } else
            throw new UnexpectedExtensionsException("Check the input file or the output file extensions");

    }

}
