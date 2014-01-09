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
import eu.europeana.cloud.common.model.DataProviderProperties;

/**
 * Test Create id mappings with one provider
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class TestCreateMappingIdOneProviderCommand extends Command {

	@Override
	public void execute(UISClient client, int threadNo,String... input) throws InvalidAttributesException {
		String providerId = input[1]+threadNo;
		String recordId = input[2];
		
		try {
			client.createProvider(providerId+"initial1", new DataProviderProperties());
			client.createProvider(providerId, new DataProviderProperties());
			CloudId cId = client.createCloudId(providerId+"initial1",recordId+"initial1");
			long i=0;
			List<String> str = new ArrayList<>();
			Date now = new Date();
			long start = now.getTime();
			System.out.println("Test started at: " + now.toString());
			
			while(i<Long.parseLong(input[0])){
				client.createMapping(cId.getId(),providerId,recordId+i);
				str.add(String.format("%s %s %s", cId.getId(),providerId,recordId+i));
				i++;
			}
			long end = new Date().getTime() - start;
			System.out.println("Adding "+ input[0]+" records took " + end + " ms");
			System.out.println("Average: " + (Double.parseDouble(input[0])/end) *1000 +" records per second");
			IOUtils.writeLines(str, "\n", new FileOutputStream(new File(input[3])));
			
		} catch (CloudException | IOException e) {
			e.printStackTrace();
		}

	}

}
