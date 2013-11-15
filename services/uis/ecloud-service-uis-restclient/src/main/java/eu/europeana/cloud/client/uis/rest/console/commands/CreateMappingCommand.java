package eu.europeana.cloud.client.uis.rest.console.commands;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.Command;

/**
 * Create a new mapping console command
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public class CreateMappingCommand extends Command {

	@Override
	public void execute(UISClient client,String... input) {
		try {
			System.out.println(client.createMapping(input[0], input[1], input[2]));
		} catch (CloudException e) {
			System.out.println(e.getMessage());
		}
	}

}
