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

public class CreateCloudIdBatchCommand extends Command {

	@Override
	public void execute(UISClient client, String... input) throws InvalidAttributesException {
		
		try {
			List<String> ids = FileUtils.readLines(new File(input[0]));
			List<String> created = new ArrayList<>();
			
			for (String id : ids){
				String[] info = id.split(" ");
				CloudId cId = client.createCloudId(info[0], info[1]);
				created.add(String.format("%s %s %s", cId.getId(),info[0],info[1]));
			}
			FileUtils.writeLines(new File("batch_no_generation"), created);
		} catch (IOException | CloudException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
