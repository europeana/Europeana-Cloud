package eu.europeana.cloud.client.uis.rest.console.commands;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProviderProperties;
import org.apache.commons.io.FileUtils;

import javax.naming.directory.InvalidAttributesException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Generate Batch Identifiers according to a file
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class CreateCloudIdBatchCommand extends Command {

	@Override
	public void execute(UISClient client, int threadNo, String... input) throws InvalidAttributesException {
		
		try {
			List<String> ids = FileUtils.readLines(new File(input[0]));
			List<String> created = new ArrayList<>(ids.size());
			client.createProvider(ids.get(0).split(" ")[0], new DataProviderProperties());
			for (String id : ids){
				String[] info = id.split(" ");
				CloudId cId = client.createCloudId(info[0], info[1]);
				created.add(String.format("%s %s %s", cId.getId(),info[0],info[1]));
			}
			FileUtils.writeLines(new File("batch_no_generation"), created);
		} catch (IOException | CloudException e) {
			getLogger().error(e.getMessage());
		}
	}

}
