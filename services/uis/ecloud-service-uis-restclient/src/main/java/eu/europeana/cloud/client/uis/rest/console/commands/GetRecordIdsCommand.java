package eu.europeana.cloud.client.uis.rest.console.commands;

import javax.naming.directory.InvalidAttributesException;

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
	public void execute(UISClient client,int threadNo,String... input) throws InvalidAttributesException{
		if(input.length<1){
			throw new InvalidAttributesException();
		}
		try {
			for (CloudId cId : client.getRecordId(input[0]).getResults()) {
				System.out.println(cId.toString());
			}
		} catch (CloudException e) {
			System.out.println(e.getMessage());
		}

	}

}
