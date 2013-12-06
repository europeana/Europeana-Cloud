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

public class TestDeleteCommand extends Command {

	@Override
	public void execute(UISClient client, String... input) throws InvalidAttributesException {
		try {
			List<String> ids = FileUtils.readLines(new File("tests1IdRW"));
			Date now = new Date();
			System.out.println("Starting test at: " + now.toString());
			for (String id : ids) {
				String[] columns = id.split(" ");
				client.deleteCloudId(columns[0]);
			}
			long end = new Date().getTime() - now.getTime();
			System.out.println("Fetching " + ids.size() + " records took " + end + " ms");
			System.out.println("Average: " + (ids.size() / end) * 1000 + " records per second");
		} catch (IOException | CloudException e) {
			e.printStackTrace();
		}

	}

}
