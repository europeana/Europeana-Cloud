package eu.europeana.cloud.client.uis.rest.console;

import java.util.HashMap;
import java.util.Map;

import javax.naming.directory.InvalidAttributesException;

import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.commands.CreateCloudIdBatchCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.CreateCloudIdBatchWithGenerationCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.CreateCloudIdCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.CreateMappingCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.DeleteCloudIdCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.GetCloudIdCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.GetCloudIdsByProviderCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.GetCloudIdsByProviderWithPaginationCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.GetRecordIdsByProviderCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.GetRecordIdsByProviderWithPaginationCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.GetRecordIdsCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.HelpCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.RemoveMappingByLocalIdCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.TestCreateMappingIdManyProvidersCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.TestCreateMappingIdOneProviderCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.TestDeleteCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.TestReadManyProvidersCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.TestReadOneProviderCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.TestReadWriteManyProvidersCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.TestReadWriteManyProvidersWithIdCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.TestReadWriteOneProviderCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.TestReadWriteOneProviderWithIdCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.TestRetrieveCloudIdCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.TestRetrieveCloudIdNoPaginationCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.TestRetrieveCloudIdWithPaginationCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.TestRetrieveLocalIdNoPaginationCommand;
import eu.europeana.cloud.client.uis.rest.console.commands.tests.TestRetrieveLocalIdWithPaginationCommand;

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
		UISClient client = new UISClient();
		

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
		return new HashMap<String, Command>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -7743782749216945386L;

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
				put("createBatchCloudId",new CreateCloudIdBatchCommand());
				put("createBatchCloudWithGeneration", new CreateCloudIdBatchWithGenerationCommand());
				put("removeMappingByLocalId", new RemoveMappingByLocalIdCommand());
				put("testOneProviderRW", new TestReadWriteOneProviderCommand());
				put("testManyProvidersRW", new TestReadWriteManyProvidersCommand());
				put("testManyProvidersWithIdRW", new TestReadWriteManyProvidersWithIdCommand());
				put("testOneProviderWithIdRW", new TestReadWriteOneProviderWithIdCommand());
				put("testOneProviderWrite",new TestCreateMappingIdOneProviderCommand());
				put("testManyProvidersWrite",new TestCreateMappingIdManyProvidersCommand());
				put("testReadOneProvider",new TestReadOneProviderCommand());
				put("testReadManyProviders",new TestReadManyProvidersCommand());
				put("testRetrieveIdsByCloudId", new TestRetrieveCloudIdCommand());
				put("testRetrieveCloudIdByProvider", new TestRetrieveCloudIdNoPaginationCommand());
				put("testRetrieveCloudIdByProviderWithPagination", new TestRetrieveCloudIdWithPaginationCommand());
				put("testRetrieveLocalIdByProvider", new TestRetrieveLocalIdNoPaginationCommand());
				put("testRetrieveLocalIdByProviderWithPagination", new TestRetrieveLocalIdWithPaginationCommand());
				put("testDeleteCloudId",new TestDeleteCommand());
				put("help", new HelpCommand());
				put("?", new HelpCommand());
			}

		};
	}

	private static String[] subArray(String[] input) throws InvalidAttributesException {
		String[] ret = new String[input.length - 1];
		System.arraycopy(input, 1, ret, 0, ret.length);
		return ret;
	}
	
	/**
	 * Inject command line parameters
	 * @param input
	 */
	public void setInput(String[] input){
		this.input=input.clone();
	}
	
	/**
	 * Set the thread id
	 * @param id
	 */
	public void setId(int id){
		this.id = id;
	}
}
