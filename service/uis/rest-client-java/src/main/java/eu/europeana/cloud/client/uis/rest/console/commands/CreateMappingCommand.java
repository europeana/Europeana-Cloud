package eu.europeana.cloud.client.uis.rest.console.commands;

import javax.naming.directory.InvalidAttributesException;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.Command;
import eu.europeana.cloud.common.model.DataProviderProperties;

/**
 * Create a new mapping console command
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public class CreateMappingCommand extends Command {

	@Override
	public void execute(UISClient client,int threadNo,String... input) throws InvalidAttributesException{
		if(input.length<3){
			throw new InvalidAttributesException();
		}
		try {
			client.createProvider(input[1], new DataProviderProperties());
			System.out.println(client.createMapping(input[0], input[1], input[2]));
		} catch (CloudException e) {
			System.out.println(e.getMessage());
		}
	}

}
