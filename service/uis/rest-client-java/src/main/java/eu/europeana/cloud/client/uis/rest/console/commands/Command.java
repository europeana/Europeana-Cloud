package eu.europeana.cloud.client.uis.rest.console.commands;

import javax.naming.directory.InvalidAttributesException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.client.uis.rest.UISClient;

/**
 * Abstract console command
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public abstract class Command {

	private static final Logger LOGGER = LoggerFactory.getLogger(Command.class);
	
	
	/**
	 * Get parent logger
	 * @return The logger
	 */
	public Logger getLogger(){
		return LOGGER;
	}
	/**
	 * Execution method of the command
	 * @param client The UISClient to connect to
	 * @param threadNo The thread identifier
	 * @param input The command line parameters
	 * @throws InvalidAttributesException 
	 */
	public abstract void execute(UISClient client, int threadNo, String... input) throws InvalidAttributesException;

}
