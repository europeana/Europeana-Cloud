package eu.europeana.cloud.client.uis.rest.console;

import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.commands.*;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.*;

import javax.naming.directory.InvalidAttributesException;
import java.util.HashMap;
import java.util.Map;

/**
 * Command line application that uses the REST API Client
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public class App implements Runnable {

	private String[] input;
	private int id;

	@Override
	public void run() {
		UISClient client = new UISClient("http://cloud.europeana.eu");

		Map<String, Command> commands = populateCommands();
		boolean supported = true;

		if (!commands.containsKey(input[0])) {
			System.out.println("Operation is not supported");
			supported = false;
		}
		if (supported) {
			try {
				commands.get(input[0]).execute(client, id, input.length > 1 ? subArray(input) : new String[0]);
			} catch (InvalidAttributesException e) {
				System.out.println("Wrong number of arguments provided");
			}
		}

	}

	private static Map<String, Command> populateCommands() {
		Map<String,Command> a = new HashMap<>();

		a.put("createCloudId", new CreateCloudIdCommand());
		a.put("createMapping", new CreateMappingCommand());
		a.put("deleteCloudId", new DeleteCloudIdCommand());
		a.put("getCloudId", new GetCloudIdCommand());
		a.put("getCloudIdsByProvider", new GetCloudIdsByProviderCommand());
		a.put("getCloudIdsByProviderWithPagination", new GetCloudIdsByProviderWithPaginationCommand());
		a.put("getRecordIdsByProvider", new GetRecordIdsByProviderCommand());
		a.put("getRecordIdsByProviderWithPagination", new GetRecordIdsByProviderWithPaginationCommand());
		a.put("getRecordIds", new GetRecordIdsCommand());
		a.put("createBatchCloudId", new CreateCloudIdBatchCommand());
		a.put("createBatchCloudWithGeneration", new CreateCloudIdBatchWithGenerationCommand());
		a.put("removeMappingByLocalId", new RemoveMappingByLocalIdCommand());
		a.put("testOneProviderRW", new TestReadWriteOneProviderCommand());
		a.put("testManyProvidersRW", new TestReadWriteManyProvidersCommand());
		a.put("testManyProvidersWithIdRW", new TestReadWriteManyProvidersWithIdCommand());
		a.put("testOneProviderWithIdRW", new TestReadWriteOneProviderWithIdCommand());
		a.put("testOneProviderWrite", new TestCreateMappingIdOneProviderCommand());
		a.put("testManyProvidersWrite", new TestCreateMappingIdManyProvidersCommand());
		a.put("testReadOneProvider", new TestReadOneProviderCommand());
		a.put("testReadManyProviders", new TestReadManyProvidersCommand());
		a.put("testRetrieveIdsByCloudId", new TestRetrieveCloudIdCommand());
		a.put("testRetrieveCloudIdByProvider", new TestRetrieveCloudIdNoPaginationCommand());
		a.put("testRetrieveCloudIdByProviderWithPagination", new TestRetrieveCloudIdWithPaginationCommand());
		a.put("testRetrieveLocalIdByProvider", new TestRetrieveLocalIdNoPaginationCommand());
		a.put("testRetrieveLocalIdByProviderWithPagination", new TestRetrieveLocalIdWithPaginationCommand());
		a.put("testDeleteCloudId", new TestDeleteCommand());
		a.put("help", new HelpCommand());
		a.put("?", new HelpCommand());

		return a;
	}

	private static String[] subArray(String[] input) throws InvalidAttributesException {
		String[] ret = new String[input.length - 1];
		System.arraycopy(input, 1, ret, 0, ret.length);
		return ret;
	}

	/**
	 * Inject command line parameters
	 * 
	 * @param input
	 */
	public void setInput(String[] input) {
		this.input = input.clone();
	}

	/**
	 * Set the thread id
	 * 
	 * @param id
	 */
	public void setId(int id) {
		this.id = id;
	}
}
