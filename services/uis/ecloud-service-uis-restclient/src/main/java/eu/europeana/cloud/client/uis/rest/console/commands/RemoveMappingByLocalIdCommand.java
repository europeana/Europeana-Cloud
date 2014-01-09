package eu.europeana.cloud.client.uis.rest.console.commands;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.Command;

/**
 * Removal of a record id mapping console command
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class RemoveMappingByLocalIdCommand extends Command {

	@Override
	public void execute(UISClient client,int threadNo,String... input) {
		try{
			System.out.println(client.removeMappingByLocalId(input[0], input[1]));
		} catch (CloudException e){
			System.out.println(e.getMessage());
		}
	}

}
