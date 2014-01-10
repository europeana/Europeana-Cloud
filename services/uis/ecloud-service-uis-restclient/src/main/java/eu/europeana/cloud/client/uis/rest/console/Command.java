package eu.europeana.cloud.client.uis.rest.console;

import javax.naming.directory.InvalidAttributesException;

import eu.europeana.cloud.client.uis.rest.UISClient;

/**
 * Abstract console command
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public abstract class Command {

	/**
	 * Execution method of the command
	 * @param client The UISClient to connect to
	 * @param threadNo The thread identifier
	 * @param input The command line parameters
	 * @throws InvalidAttributesException 
	 */
	public abstract void execute(UISClient client, int threadNo, String... input) throws InvalidAttributesException;

}
