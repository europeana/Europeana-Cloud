package eu.europeana.cloud.client.uis.rest.console.commands;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.Command;
import eu.europeana.cloud.common.model.CloudId;

/**
 * Create a new CloudId console command
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public class CreateCloudIdCommand extends Command {

	@Override
	public void execute(UISClient client, String... input) {
		try {
			CloudId cId = client.createCloudId(input[0], input[1]);
			System.console().writer().println(cId.toString());
		} catch (CloudException e) {
			System.console().writer().println(e.getMessage());
		}

	}

}
