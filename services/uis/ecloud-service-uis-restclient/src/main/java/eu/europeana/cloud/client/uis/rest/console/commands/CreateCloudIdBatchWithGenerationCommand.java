package eu.europeana.cloud.client.uis.rest.console.commands;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.naming.directory.InvalidAttributesException;

import org.apache.commons.io.FileUtils;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.Command;
import eu.europeana.cloud.common.model.CloudId;

public class CreateCloudIdBatchWithGenerationCommand extends Command {

	@Override
	public void execute(UISClient client, String... input) throws InvalidAttributesException {
		
		try {
			
			List<String> created = new ArrayList<>();
			int i=0;
			while(i<Integer.parseInt(input[1])){
				
				CloudId cId = client.createCloudId(input[0]);
				created.add(String.format("%s %s %s", cId.getId(),input[0],cId.getLocalId().getRecordId()));
				i++;
			}
			FileUtils.writeLines(new File("batch_with_generation"), created);
		} catch (CloudException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
