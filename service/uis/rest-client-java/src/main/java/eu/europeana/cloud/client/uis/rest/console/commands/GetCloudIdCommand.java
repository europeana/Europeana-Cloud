package eu.europeana.cloud.client.uis.rest.console.commands;

import javax.naming.directory.InvalidAttributesException;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;

/**
 * Retrieval of a cloud id console command
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class GetCloudIdCommand extends Command {

	@Override
	public void execute(UISClient client,int threadNo,String... input) throws InvalidAttributesException{
		if(input.length<2){
			throw new InvalidAttributesException();
		}
		try{
			CloudId cId = client.getCloudId(input[0], input[1]);
			System.out.println(cId.toString());
		} catch (CloudException e){
			getLogger().error(e.getMessage());
		}

	}

}
