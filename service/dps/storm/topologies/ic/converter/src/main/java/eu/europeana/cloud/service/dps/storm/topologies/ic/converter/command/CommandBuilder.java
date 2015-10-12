package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.command;

import java.util.List;

/**
 * Utility for building a full-fledged command based on input parameters
 */
public interface CommandBuilder {
	/**
	 * Build full command based on input parameters
	 *
	 * @param inputFilePath        The input file full path
	 * @param outputFilePath       The output file full path
	 * @param properties           List of properties attached to the kakadu command
	 * @return Full-fledged command .
	 */
	public  String constructCommand(String inputFilePath,
									String outputFilePath, List<String> properties);

}
