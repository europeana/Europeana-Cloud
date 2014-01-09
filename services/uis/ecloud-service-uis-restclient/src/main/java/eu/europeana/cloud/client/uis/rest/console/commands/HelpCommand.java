package eu.europeana.cloud.client.uis.rest.console.commands;

import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.client.uis.rest.console.Command;

/**
 * Help message
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class HelpCommand extends Command {

	@Override
	public void execute(UISClient client, int threadNo,String... input) {
		StringBuffer sb = new StringBuffer();
		sb.append("Supported operations are:\n");
		sb.append("createCloudId\t\t\tproviderId [recordId]: Create a new cloud id\n");
		sb.append("createBatchCloudId\t\t\tfile: Create batch cloud ids from a file. The file should provider a line per provider and record id spaced delimited\n");
		sb.append("createBatchCloudIdWithGeneration\t\t\tproviderId # of CloudIds: Create batch cloud ids for a provider.\n");
		sb.append("createMapping\t\t\tcloudId providerId recordId: Create a mapping between a cloudId and a record Id\n");
		sb.append("deleteCloudId\t\t\tcloudId: Delete a cloudId\n");
		sb.append("getCloudId\t\t\tproviderId recordId: Retrieve a cloudId from a recordId\n");
		sb.append("getCloudIdsByProvider\t\t\tproviderId: Retrieve the cloudIds for a provider\n");
		sb.append("getCloudIdsByProviderWithPagination\t\t\tproviderId recordId to: Retrieve the cloudIds for a provider with pagination\n");
		sb.append("getRecordIdsByProvider\t\t\tproviderId: Retrieve the recordIds for a provider\n");
		sb.append("getRecordIdsByProviderWithPagination\t\t\tproviderId recordId to: Retrieve the recordIds for a provider with pagination\n");
		sb.append("getRecordIds\t\t\tcloudId: Retrieve record ids associated with a CloudId\n");
		sb.append("removeMappingByLocalId\t\t\tcloudId,providerId,recordId: Remove the mapping between a cloudId and a recordId\n");
		sb.append("help, ?\t\t\t\tPrint this message\n");
		sb.append("\n\n\n");
		sb.append("Test Suite\n");
		sb.append("testOneProviderRW\t\t\t Create Ids for one provider with automatic local Id generation\n");
		sb.append("testManyProvidersRW\t\t\t Create Ids for many provider with automatic local Id generation\n");
		sb.append("testManyProvidersWithIdRW\t\t\t Create Ids for many provider with manual local Id generation\n");
		sb.append("testOneProviderWithIdRW\t\t\t Create Ids for one provider with manual local Id generation\n");
		sb.append("testOneProviderWrite\t\t\t Create Id mapping for one provider\n");
		sb.append("testManyProvidersWrite\t\t\t Create Id mapping for many providers\n");
		sb.append("testReadOneProvider\t\t\t Read cloud ids for a single provider with record id\n");
		sb.append("testReadManyProviders\t\t\t Read cloud ids for many providers with record id\n");
		sb.append("testRetrieveIdsByCloudId\t\t\t Read local ids by cloud id\n");
		sb.append("testRetrieveCloudIdByProvider\t\t\t Read cloud ids for a provider no pagination\n");
		sb.append("testRetrieveCloudIdByProviderWithPagination\t\t\t Read cloud ids for a provider with pagination\n");
		sb.append("testRetrieveLocalIdByProvider\t\t\t Read local ids for a provider no pagination\n");
		sb.append("testRetrieveLocalIdByProviderWithPagination\t\t\t Read local ids for a provider with pagination\n");
		sb.append("testDeleteCloudId\t\t\t Delete cloud identifiers\n");
		System.out.println(sb.toString());
	}

}
