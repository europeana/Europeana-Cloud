package eu.europeana.cloud.client.uis.rest.console.commands.tests;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import javax.naming.directory.InvalidAttributesException;

import org.apache.commons.io.FileUtils;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.Command;
import eu.europeana.cloud.common.model.CloudId;

public class TestRetrieveCloudIdWithPaginationCommand extends Command {

	@Override
	public void execute(UISClient client, String... input) throws InvalidAttributesException {
		try {
			List<String> ids = FileUtils.readLines(new File("tests1IdRW"));
			String[] columns = ids.get(0).split(" ");
			List<CloudId> cloudIds = client.getCloudIdsByProvider(columns[1]);
			int window = Integer.parseInt(input[0]);
			Date now = new Date();

			System.out.println("Starting test at: " + now.toString());
			List<CloudId> paginated = client.getCloudIdsByProviderWithPagination(columns[1], cloudIds.get(0)
					.getId(), window);
			while (paginated.size()==window){
				List<CloudId>paginated2 = client.getCloudIdsByProviderWithPagination(columns[1], paginated.get(19)
						.getId(), window);
				paginated = paginated2;
			}
			long end = new Date().getTime() - now.getTime();
			System.out.println("Fetching " + cloudIds.size() + "with window " +window+ " records took " + end + " ms");
			System.out.println("Average: " + (cloudIds.size() / end) * 1000 + " records per second");
		} catch (IOException | CloudException e) {
			e.printStackTrace();
		}

	}

}
