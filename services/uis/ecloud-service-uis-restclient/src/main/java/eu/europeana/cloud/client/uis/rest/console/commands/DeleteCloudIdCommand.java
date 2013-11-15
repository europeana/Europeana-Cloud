package eu.europeana.cloud.client.uis.rest.console.commands;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.Command;

/**
 * Delete a cloud id console command
 * @author Yorgos.MAmakis@ kb.nl
 *
 */
public class DeleteCloudIdCommand extends Command {

	@Override
	public void execute(UISClient client,String... input) {
		try{
			System.out.println(client.deleteCloudId(input[0]));
		} catch (CloudException e){
			System.out.println(e.getMessage());
		}
	}

}
