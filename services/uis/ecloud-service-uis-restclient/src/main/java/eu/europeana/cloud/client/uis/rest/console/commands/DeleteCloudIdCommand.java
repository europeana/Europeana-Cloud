package eu.europeana.cloud.client.uis.rest.console.commands;

import javax.naming.directory.InvalidAttributesException;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.Command;

/**
 * Delete a cloud id console command
 * @author Yorgos.MAmakis@ kb.nl
 *
 */
public class DeleteCloudIdCommand extends Command {

	@Override
	public void execute(UISClient client,int threadNo,String... input) throws InvalidAttributesException{
		if(input.length<1){
			throw new InvalidAttributesException();
		}
		try{
			System.out.println(client.deleteCloudId(input[0]));
		} catch (CloudException e){
			System.out.println(e.getMessage());
		}
	}

}
