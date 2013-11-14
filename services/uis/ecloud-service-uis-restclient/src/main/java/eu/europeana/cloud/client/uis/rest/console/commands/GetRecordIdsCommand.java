package eu.europeana.cloud.client.uis.rest.console.commands;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.Command;
import eu.europeana.cloud.common.model.CloudId;

/**
 * Retrieval of record ids from cloud id console command
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class GetRecordIdsCommand extends Command {

	@Override
	public void execute(String... input) {
		try {
			for (CloudId cId : UISClient.getRecordId(input[0])) {
				System.out.println(cId.toString());
			}
		} catch (CloudException e) {
			System.out.println(e.getMessage());
		}

	}

}
