package eu.europeana.cloud.client.uis.rest.console.commands;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.Command;
import eu.europeana.cloud.common.model.LocalId;


/**
 * Retrieval of record ids by provider with pagination console command
 * @author ecloud
 *
 */
public class GetRecordIdsByProviderWithPaginationCommand extends Command {

	@Override
	public void execute(String... input) {
		try {
			for (LocalId cId : UISClient.getRecordIdsByProviderWithPagination(input[0],input[1],Integer.parseInt(input[2]))) {
				System.out.println(cId.toString());
			}
		} catch (CloudException e) {
			System.out.println(e.getMessage());
		}
		
	}

}
