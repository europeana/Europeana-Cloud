package eu.europeana.cloud.client.uis.rest.console.commands.tests;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import javax.naming.directory.InvalidAttributesException;

import org.apache.commons.io.FileUtils;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.commands.Command;

/**
 * Test read Ids from one provider
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class TestReadOneProviderCommand extends Command {

	@Override
	public void execute(UISClient client, int threadNo,String... input) throws InvalidAttributesException {
		try {
			List<String> ids = FileUtils.readLines(new File("tests1IdRW"));
			Date now = new Date();
			System.out.println("Starting test at: " + now.toString());
			for(String id:ids){
				String[] columns = id.split(" ");
				client.getCloudId(columns[1], columns[2]);
			}
			long end = new Date().getTime()-now.getTime();
			System.out.println("Reading "+ ids.size()+" records took " + end + " ms");
			System.out.println("Average: " + (ids.size()/end) *1000 +" records per second");
		} catch (IOException | CloudException e) {
			getLogger().error(e.getMessage());
		}
		

	}

}
