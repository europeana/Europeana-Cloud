package eu.europeana.cloud.client.uis.rest.console;

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
	public abstract void execute(String... input);

}
