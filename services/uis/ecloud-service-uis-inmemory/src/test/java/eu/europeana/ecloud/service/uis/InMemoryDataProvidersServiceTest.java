/* InMemoryDataProvidersServiceTest.java - created on Jan 6, 2014, Copyright (c) 2013 Europeana Foundation, all rights reserved */
package eu.europeana.ecloud.service.uis;


import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.uis.InMemoryDataProvidersService;
import eu.europeana.cloud.service.uis.dao.InMemoryDataProviderDAO;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;

/**
 * InMemory data provider service tests
 * 
 * @author Yorgos Mamakis (Yorgos.Mamakis@ europeana.eu)
 * @since Jan 6, 2014
 */
public class InMemoryDataProvidersServiceTest {

	/**
	 * Creates a new instance of this class.
	 */
	public InMemoryDataProvidersServiceTest() {

	}

	private InMemoryDataProvidersService service;
	private final static String PROVIDERID = "providerId";
	private final static String ORGANIZATIONNAME = "org";
	private final static String OFFICIALADDRESS = "address";
	private final static String ORGANIZATIONWEBSITE = "organizationws";
	private final static String ORGANIZATIONWEBSITEURL = "organizationwsURL";
	private final static String DIGITALLIBRARY = "digitalLibraryws";
	private final static String DIGITALLIBRARYURL = "digitalLibrarywsurl";
	private final static String CONTACTPERSON = "contact";
	private final static String REMARKS = "remarks";

	/**
	 * Prepare Unit tests
	 */
	@Before
	public void prepare() {
		service = new InMemoryDataProvidersService(new InMemoryDataProviderDAO());
	}

	/**
	 * Test Create Provider
	 * @throws ProviderAlreadyExistsException
	 */
	@Test
	public void testCreateProvider() throws ProviderAlreadyExistsException {
		DataProviderProperties orig = createDataProviderProperties();
		DataProvider dp = service.createProvider(PROVIDERID, orig);
		assertEquals(dp.getId(),PROVIDERID);
		assertEquals(dp.getProperties(),orig);
	}

	/**
	 * Test Create Provider Exception exists
	 * @throws ProviderAlreadyExistsException
	 */
	
	@Test (expected = ProviderAlreadyExistsException.class)
	public void testCreateProviderAlreadyExists() throws ProviderAlreadyExistsException {
		DataProviderProperties orig = createDataProviderProperties();
		service.createProvider(PROVIDERID, orig);
		service.createProvider(PROVIDERID, orig);
	}
	
	/**
	 * Test Get Provider with exception
	 * @throws ProviderDoesNotExistException 
	 */
	@Test(expected = ProviderDoesNotExistException.class)
	public void testGetProviderDoesNotExist() throws ProviderDoesNotExistException{
		service.getProvider(PROVIDERID);
	}
	
	/**
	 * Test Get Provider
	 * @throws ProviderDoesNotExistException 
	 * @throws ProviderAlreadyExistsException 
	 */
	@Test
	public void testGetProvider() throws ProviderDoesNotExistException, ProviderAlreadyExistsException{
		DataProviderProperties orig = createDataProviderProperties();
		DataProvider dp = service.createProvider(PROVIDERID, orig);
		DataProvider retDp = service.getProvider(PROVIDERID);
		assertEquals(dp,retDp);
	}
	
	/**
	 * Test Get Provider with exception
	 * @throws ProviderDoesNotExistException 
	 */
	@Test(expected = ProviderDoesNotExistException.class)
	public void testUpdateProviderDoesNotExist() throws ProviderDoesNotExistException{
		service.updateProvider(PROVIDERID,new DataProviderProperties());
	}
	
	/**
	 * Test Get Provider
	 * @throws ProviderDoesNotExistException 
	 * @throws ProviderAlreadyExistsException 
	 */
	@Test
	public void testUpdateProvider() throws ProviderDoesNotExistException, ProviderAlreadyExistsException{
		DataProviderProperties orig = createDataProviderProperties();
		DataProvider dp = service.createProvider(PROVIDERID, new DataProviderProperties());
		DataProvider retDp = service.getProvider(PROVIDERID);
		assertEquals(dp,retDp);
		DataProvider dpUp = service.updateProvider(PROVIDERID, orig);
		retDp = service.getProvider(PROVIDERID);
		assertEquals(dpUp, retDp);
	}
	
	/**
	 * Test Get Provider with exception
	 * @throws ProviderDoesNotExistException 
	 */
	@Test(expected = UnsupportedOperationException.class)
	public void testGetProvidersUnsupported() {
		service.getProviders(PROVIDERID, 1);
	}
	
	/**
	 * Test Get Provider
	 * @throws ProviderDoesNotExistException 
	 * @throws ProviderAlreadyExistsException 
	 */
	@Test
	public void testGetProviders() throws ProviderDoesNotExistException, ProviderAlreadyExistsException{
		DataProvider dp = service.createProvider(PROVIDERID, new DataProviderProperties());
		ResultSlice<DataProvider> retDp = service.getProviders(null,1);
		assertEquals(retDp.getResults().size(),1);
		assertEquals(retDp.getResults().get(0), dp);
	}
	
	
	/**
	 * @return 
	 */
	private DataProviderProperties createDataProviderProperties() {
		return new DataProviderProperties(ORGANIZATIONNAME, OFFICIALADDRESS, ORGANIZATIONWEBSITE,
				ORGANIZATIONWEBSITEURL, DIGITALLIBRARY, DIGITALLIBRARYURL, CONTACTPERSON, REMARKS);
	}

}
