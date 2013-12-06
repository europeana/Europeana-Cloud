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
	 * @param input The command line parameters
	 */
	public abstract void execute(UISClient client, String... input) throws InvalidAttributesException;

}
