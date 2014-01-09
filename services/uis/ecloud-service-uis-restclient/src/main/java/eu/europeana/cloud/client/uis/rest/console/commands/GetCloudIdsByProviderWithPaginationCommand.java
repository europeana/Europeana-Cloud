package eu.europeana.cloud.client.uis.rest.console.commands;

import javax.naming.directory.InvalidAttributesException;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.Command;
import eu.europeana.cloud.common.model.CloudId;

/**
 * Retrieval of cloud ids with pagination
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class GetCloudIdsByProviderWithPaginationCommand extends Command {
	@Override
	public void execute(UISClient client,int threadNo,String... input) throws InvalidAttributesException{
		if(input.length<3){
			throw new InvalidAttributesException();
		}
		try {
			for (CloudId cId : client.getCloudIdsByProviderWithPagination(input[0], input[1],
					Integer.parseInt(input[2]))) {
				System.out.println(cId.toString());
			}
		} catch (CloudException e) {
			System.out.println(e.getMessage());
		}

	}
}
