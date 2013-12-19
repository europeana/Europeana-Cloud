package eu.europeana.cloud.client.uis.rest.console.commands.tests;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.naming.directory.InvalidAttributesException;

import org.apache.commons.io.IOUtils;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.Command;
import eu.europeana.cloud.common.model.CloudId;

/**
 * Test Read Write one provider
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class TestReadWriteOneProviderCommand extends Command {

	@Override
	public void execute(UISClient client, String... input) throws InvalidAttributesException {
		
		String providerId = input[1];
		try {
			long i=0;
			List<String> str = new ArrayList<>();
			Date now = new Date();
			long start = now.getTime();
			System.out.println("Test started at: " + now.toString());
			while(i<Long.parseLong(input[0])){
				CloudId cId = client.createCloudId(providerId);
				str.add(String.format("%s %s %s", cId.getId(),cId.getLocalId().getProviderId(),cId.getLocalId().getRecordId()));
				i++;
			}
			long end = new Date().getTime() - start;
			System.out.println("Adding "+ input[0]+" records took " + end + " ms");
			System.out.println("Average: " + (Double.parseDouble(input[0])/end) *1000 +" records per second");
			IOUtils.writeLines(str, "\n", new FileOutputStream(new File(input[2])));
		} catch (CloudException | IOException e) {
			e.printStackTrace();
		}
		
	}

}
