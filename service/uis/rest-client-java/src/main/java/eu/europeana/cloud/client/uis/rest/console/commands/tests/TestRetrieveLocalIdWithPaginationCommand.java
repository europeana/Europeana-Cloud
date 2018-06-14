package eu.europeana.cloud.client.uis.rest.console.commands.tests;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.commands.Command;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import org.apache.commons.io.FileUtils;

import javax.naming.directory.InvalidAttributesException;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Test retrieve local Id without pagination
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class TestRetrieveLocalIdWithPaginationCommand extends Command {

	@Override
	public void execute(UISClient client, int threadNo,String... input) throws InvalidAttributesException {
		try {
			List<String> ids = FileUtils.readLines(new File(input[1]));
			String[] columns = ids.get(0).split(" ");
			List<CloudId> cloudIds = client.getCloudIdsByProvider(columns[1]).getResults();
			int window = Integer.parseInt(input[0]);
			Date now = new Date();

			System.out.println("Starting test at: " + now);
			List<LocalId> paginated = client.getRecordIdsByProviderWithPagination(columns[1], cloudIds.get(0)
					.getLocalId().getRecordId(), window).getResults();
			while (paginated.size()==window){
				List<LocalId>paginated2 = client.getRecordIdsByProviderWithPagination(columns[1], paginated.get(19)
						.getRecordId(), window).getResults();
				paginated = paginated2;
			}
			long end = new Date().getTime() - now.getTime();
			System.out.println("Fetching " + cloudIds.size() + " records " + window +" took " + end + " ms");
			System.out.println("Average: " + (cloudIds.size() / end) * 1000 + " records per second");
		} catch (IOException | CloudException e) {
			getLogger().error(e.getMessage());
		}

	}

}
