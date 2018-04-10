package eu.europeana.cloud.client.uis.rest.console.commands;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;

import javax.naming.directory.InvalidAttributesException;

/**
 * Retrieval of a cloud id by a provider console command
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class GetCloudIdsByProviderCommand extends Command {

	@Override
	public void execute(UISClient client,int threadNo,String... input) throws InvalidAttributesException{
		if(input.length<1){
			throw new InvalidAttributesException();
		}
		try {
			for (CloudId cId : client.getCloudIdsByProvider(input[0]).getResults()) {
				System.out.println(cId.toString());
			}
		} catch (CloudException e) {
			getLogger().error(e.getMessage());
		}

	}

}
