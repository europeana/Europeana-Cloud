package eu.europeana.cloud.client.uis.rest.console.commands;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.Command;
import eu.europeana.cloud.common.model.LocalId;

/**
 * Retrieval of record ids by provider console command
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class GetRecordIdsByProviderCommand extends Command {

	@Override
	public void execute(String... input) {
		try {
			for (LocalId cId : UISClient.getRecordIdsByProvider(input[0])) {
				System.out.println(cId.toString());
			}
		} catch (CloudException e) {
			System.out.println(e.getMessage());
		}

	}

}
