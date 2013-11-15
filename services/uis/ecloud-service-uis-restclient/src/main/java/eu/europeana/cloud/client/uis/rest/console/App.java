package eu.europeana.cloud.client.uis.rest.console;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import javax.naming.directory.InvalidAttributesException;

import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.commands.CreateCloudIdCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.CreateMappingCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.DeleteCloudIdCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.GetCloudIdCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.GetCloudIdsByProviderCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.GetCloudIdsByProviderWithPaginationCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.GetRecordIdsByProviderCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.GetRecordIdsByProviderWithPaginationCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.GetRecordIdsCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.RemoveMappingByLocalIdCommand;

/**
 * Command line application that uses the REST API Client
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public class App implements Runnable {

	@Override
	public void run() {
		UISClient client = new UISClient();
		Scanner scanner;
		while (true) {
			scanner = new Scanner(System.in);
			String[] input = scanner.nextLine().split(" ");

			Map<String, Command> commands = populateCommands();
			
			if(input[0].equalsIgnoreCase("exit")){
				scanner.close();
			}
			if (!commands.containsKey(input[0])) {
				throw new UnsupportedOperationException();

			}
			try {
				commands.get(input[0]).execute(client, subArray(input));
			} catch (InvalidAttributesException e) {
				e.printStackTrace();
			}
			
			
		}
		

	}

	private static Map<String, Command> populateCommands() {
		return new HashMap<String, Command>() {
			{
				put("createCloudId", new CreateCloudIdCommand());
				put("createMapping", new CreateMappingCommand());
				put("deleteCloudId", new DeleteCloudIdCommand());
				put("getCloudId", new GetCloudIdCommand());
				put("getCloudIdsByProvider", new GetCloudIdsByProviderCommand());
				put("getCloudIdsByProviderWithPagination", new GetCloudIdsByProviderWithPaginationCommand());
				put("getRecordIdsByProvider", new GetRecordIdsByProviderCommand());
				put("getRecordIdsByProviderWithPagination", new GetRecordIdsByProviderWithPaginationCommand());
				put("getRecordIds", new GetRecordIdsCommand());
				put("removeMappindByLocalId", new RemoveMappingByLocalIdCommand());
			}

		};
	}

	private static String[] subArray(String[] input) throws InvalidAttributesException {
		if (input.length == 1) {
			throw new InvalidAttributesException();
		}
		String[] ret = new String[input.length - 1];
		System.arraycopy(input, 1, ret, 0, ret.length);
		return ret;
	}
}
