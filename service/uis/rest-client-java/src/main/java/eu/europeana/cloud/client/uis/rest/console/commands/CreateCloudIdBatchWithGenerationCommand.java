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
 * Create Batch Cloud Ids without a fiel
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class CreateCloudIdBatchWithGenerationCommand extends Command {

	@Override
	public void execute(UISClient client, int threadNo, String... input) throws InvalidAttributesException {
		
		try {
			
			List<String> created = new ArrayList<>();
			int i=0;
			client.createProvider(input[0], new DataProviderProperties());
			while(i<Integer.parseInt(input[1])){
				
				CloudId cId = client.createCloudId(input[0]);
				created.add(String.format("%s %s %s", cId.getId(),input[0],cId.getLocalId().getRecordId()));
				i++;
			}
			FileUtils.writeLines(new File("batch_with_generation"), created);
		} catch (CloudException | IOException e) {
			getLogger().error(e.getMessage());
		}
	}

}
