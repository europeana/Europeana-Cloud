package eu.europeana.cloud.client.uis.rest.console.commands;

import javax.naming.directory.InvalidAttributesException;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProviderProperties;

/**
 * Create a new CloudId console command
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public class CreateCloudIdCommand extends Command {

	@Override
	public void execute(UISClient client, int threadNo, String... input) throws InvalidAttributesException {
		if (input.length < 1) {
			throw new InvalidAttributesException();
		}
		try {

			CloudId cId=null;
			client.createProvider(input[0], new DataProviderProperties());
			if (input.length == 2) {
				cId = client.createCloudId(input[0], input[1]);
			} else {
				cId = client.createCloudId(input[0]);
			}
			System.out.println(cId.toString());
		} catch (CloudException e) {
			getLogger().error(e.getMessage());
		}

	}

}
