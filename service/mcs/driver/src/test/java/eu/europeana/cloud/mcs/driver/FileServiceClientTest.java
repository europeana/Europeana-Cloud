package eu.europeana.cloud.mcs.driver;

import co.freeside.betamax.Betamax;
import co.freeside.betamax.Recorder;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.mcs.driver.exception.DriverException;

import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import java.net.URI;
import static java.util.Arrays.copyOfRange;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertArrayEquals;
import org.junit.BeforeClass;

public class FileServiceClientTest
{

	@Rule
	public Recorder recorder = new Recorder();

	//TODO clean
	//this is only needed for recording tests
	private static final String baseUrl = "http://localhost:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT";
	private static final String mediaType = "text/plain";
	private static final String cloudId = "7MZWQJF8P84";
	private static final String representationName = "schema_000001";
	private static final String version = "de084210-a393-11e3-8614-50e549e85271";

	private static final String unmovableFileName = "08fcc281-e1fd-4cec-bd33-c12a49145d36";
	private static final String deletedFileName = "d64b423b-1018-4526-ab4b-3539261ff067";
	private static final String modyfiedFileName = "12c068c9-461d-484e-878f-099c5fca447f";

	private static Client client;
	//records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/files/
	private static final String filesPath = "records/{" + ParamConstants.P_CLOUDID + "}/representations/{"
			+ ParamConstants.P_REPRESENTATIONNAME + "}/versions/{" + ParamConstants.P_VER + "}/files";
	//records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/files/FILENAME/
	private static final String filePath = filesPath + "/{" + ParamConstants.P_FILENAME + "}";


	static private void initObjects()

	{
		RecordServiceClient instanceRecord = new RecordServiceClient(baseUrl);
		FileServiceClient instanceFile = new FileServiceClient(baseUrl);
		try {
			instanceRecord.copyRepresentation(cloudId, representationName, unmovableFileName);
			for (int i = 0; i < 10; i++) {
				String contentString = "Test_123456789_" + i;
				byte[] contentBytes = contentString.getBytes("UTF-8");
				InputStream contentStream = new ByteArrayInputStream(contentBytes);
				String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
				FileServiceClient instance = new FileServiceClient(baseUrl);

				instanceFile.uploadFile(cloudId, representationName, version, contentStream, mediaType);
			}
		}
		catch (Exception ex) {
			Logger.getLogger(FileServiceClientTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}


	@BeforeClass
	public static void setUp()
		throws UnsupportedEncodingException
	{
		client = JerseyClientBuilder.newClient().register(MultiPartFeature.class);
		//initObjects();
	}


	@Betamax(tape = "files/shouldGetFileWithoutRange")
	@Test
	public void shouldGetFileWithoutRange()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		String contentString = "Test_123456789_8";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		FileServiceClient instance = new FileServiceClient(baseUrl);

		InputStream responseStream = instance.getFile(cloudId, representationName, version, unmovableFileName);

		assertNotNull(responseStream);
		byte[] responseBytes = ByteStreams.toByteArray(responseStream);
		assertArrayEquals("Content is incorrect", contentBytes, responseBytes);
		String responseChecksum = Hashing.md5().hashBytes(responseBytes).toString();
		assertEquals("Checksum is incorrect", contentChecksum, responseChecksum);
	}


	@Betamax(tape = "files/shouldGetFileWithRange1")
	@Test
	public void shouldGetFileWithRange1()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		getFileWithRange(1, 2);
	}


	@Betamax(tape = "files/shouldGetFileWithRange2")
	@Test
	public void shouldGetFileWithRange2()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		getFileWithRange(3, 14);
	}


	@Betamax(tape = "files/shouldGetFileWithRange3")
	@Test
	public void shouldGetFileWithRange3()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		getFileWithRange(0, 10);
	}


	@Betamax(tape = "files/shouldGetFileWithRange4")
	@Test
	public void shouldGetFileWithRange4()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		getFileWithRange(0, 0);
	}


	//Function required because Betamax does not compatible with multiple requests
	private static void getFileWithRange(Integer rangeStart, Integer rangeEnd)
		throws UnsupportedEncodingException, MCSException, IOException
	{
		String contentString = "Test_123456789_8";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		FileServiceClient instance = new FileServiceClient(baseUrl);
		String range = String.format("bytes=%d-%d", rangeStart, rangeEnd);

		InputStream responseStream = instance.getFile(cloudId, representationName, version, unmovableFileName, range);

		assertNotNull(responseStream);
		byte[] responseBytes = ByteStreams.toByteArray(responseStream);
		byte[] rangedContentBytes = copyOfRange(contentBytes, rangeStart, rangeEnd + 1);
		assertArrayEquals("Content is incorrect", rangedContentBytes, responseBytes);
	}


	@Betamax(tape = "files/shouldThrowWrongContentRangeExceptionForGetFileWithRangeWhenIncorrectFormat")
	@Test(expected = WrongContentRangeException.class)
	public void shouldThrowWrongContentRangeExceptionForGetFileWithRangeWhenIncorrectFormat()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		int rangeStart = 1;
		int rangeEnd = 4;
		FileServiceClient instance = new FileServiceClient(baseUrl);
		String range = String.format("bytese=%d-%d", rangeStart, rangeEnd);

		instance.getFile(cloudId, representationName, version, unmovableFileName, range);
	}


	@Betamax(tape = "files/shouldThrowWrongContentRangeExceptionForGetFileWithRangeWhenIncorrectRangeValues")
	@Test(expected = WrongContentRangeException.class)
	public void shouldThrowWrongContentRangeExceptionForGetFileWithRangeWhenIncorrectRangeValues()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		int rangeStart = 1;
		int rangeEnd = 50;
		FileServiceClient instance = new FileServiceClient(baseUrl);
		String range = String.format("bytese=%d-%d", rangeStart, rangeEnd);

		instance.getFile(cloudId, representationName, version, unmovableFileName, range);
	}


	@Betamax(tape = "files/shouldThrowFileNotExistsForGetFileWithoutRange")
	@Test(expected = FileNotExistsException.class)
	public void shouldThrowFileNotExistsForGetFileWithoutRange()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		String incorrectFileName = "edefc11e-1c5f-4a71-adb6-28efdd7b3b00";
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.getFile(cloudId, representationName, version, incorrectFileName);
	}


	@Betamax(tape = "files/shouldThrowRepresentationNotExistsForGetFileWithoutRangeWhenIncorrectCloudId")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsForGetFileWithoutRangeWhenIncorrectCloudId()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		String incorrectCloudId = "7MZWQJF8P99";
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.getFile(incorrectCloudId, representationName, version, unmovableFileName);
	}


	@Betamax(tape = "files/shouldThrowRepresentationNotExistsForGetFileWithoutRangeWhenIncorrectRepresentationName")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsForGetFileWithoutRangeWhenIncorrectRepresentationName()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		String incorrectRepresentationName = "schema_000101";
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.getFile(cloudId, incorrectRepresentationName, version, unmovableFileName);
	}


	@Betamax(tape = "files/shouldThrowRepresentationNotExistsForGetFileWithoutRangeWhenIncorrectVersion")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsForGetFileWithoutRangeWhenIncorrectVersion()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.getFile(cloudId, representationName, incorrectVersion, unmovableFileName);
	}


	@Betamax(tape = "files/shouldThrowDriverExceptionForGetFile")
	@Test(expected = DriverException.class)
	public void shouldThrowDriverExceptionForGetFile()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.getFile(cloudId, representationName, version, unmovableFileName);
	}


	@Betamax(tape = "files/shouldUploadFile")
	@Test
	public void shouldUploadFile()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		String contentString = "Test_123456789_";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		FileServiceClient instance = new FileServiceClient(baseUrl);

		URI uri = instance.uploadFile(cloudId, representationName, version, contentStream, mediaType);

		assertNotNull(uri);
		Response response = client.target(uri).request().get();
		InputStream responseStream = response.readEntity(InputStream.class);
		String responseChecksum = response.getEntityTag().toString();
		assertNotNull(responseStream);
		assertEquals("Checksum is incorrect", contentChecksum,
			responseChecksum.subSequence(1, responseChecksum.length() - 1).toString());
		assertArrayEquals("Content is incorrect", contentBytes, ByteStreams.toByteArray(responseStream));
	}


	@Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectCloudId")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectCloudId()
		throws MCSException, IOException
	{
		String contentString = "Test_123456789_";
		String incorrectCloudId = "7MZWQJS8P84";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.uploadFile(incorrectCloudId, representationName, version, contentStream, mediaType);
	}


	@Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectRepresentationName")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectRepresentationName()
		throws MCSException, IOException
	{
		String contentString = "Test_123456789_";
		String incorrectRepresentationName = "schema_000101";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.uploadFile(cloudId, incorrectRepresentationName, version, contentStream, mediaType);
	}


	@Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectVersion")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectVersion()
		throws MCSException, IOException
	{
		String contentString = "Test_123456789_";
		String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.uploadFile(cloudId, representationName, incorrectVersion, contentStream, mediaType);
	}


	@Betamax(tape = "files/shouldThrowDriverExceptionForUploadFile")
	@Test(expected = DriverException.class)
	public void shouldThrowDriverExceptionForUploadFile()
		throws MCSException, IOException
	{
		String contentString = "Test_123456789_";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.uploadFile(cloudId, representationName, version, contentStream, mediaType);
	}


	@Betamax(tape = "files/shouldThrowCannotModifyPersistentRepresentationExceptionForUploadFile")
	@Test(expected = CannotModifyPersistentRepresentationException.class)
	public void shouldThrowCannotModifyPersistentRepresentationExceptionForUploadFile()
		throws MCSException, IOException
	{
		String contentString = "Test_123456789_";
		String persistedVersion = "80441ab0-a38d-11e3-8614-50e549e85271";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.uploadFile(cloudId, representationName, persistedVersion, contentStream, mediaType);
	}


	@Betamax(tape = "files/shouldUploadFileWithChecksum")
	@Test
	public void shouldUploadFileWithChecksum()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		String contentString = "Test_123456789_1";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		FileServiceClient instance = new FileServiceClient(baseUrl);

		URI uri = instance.uploadFile(cloudId, representationName, version, contentStream, mediaType, contentChecksum);

		assertNotNull(uri);
		Response response = client.target(uri).request().get();
		InputStream responseStream = response.readEntity(InputStream.class);
		String responseChecksum = response.getEntityTag().toString();
		assertNotNull(responseStream);
		assertEquals("Checksum is incorrect", contentChecksum,
			responseChecksum.subSequence(1, responseChecksum.length() - 1).toString());
		assertArrayEquals("Content is incorrect", contentBytes, ByteStreams.toByteArray(responseStream));
	}


	@Betamax(tape = "files/shouldThrowDriverExceptionForUploadFileWithChecksum")
	@Test(expected = DriverException.class)
	public void shouldThrowDriverExceptionForUploadFileWithChecksum()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		String contentString = "Test_123456789_1";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.uploadFile(cloudId, representationName, version, contentStream, mediaType, contentChecksum);
	}


	@Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectCloudId")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectCloudId()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		String incorrectCloudId = "7MZWQJF8P00";
		String contentString = "Test_123456789_1";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.uploadFile(incorrectCloudId, representationName, version, contentStream, mediaType, contentChecksum);
	}


	@Betamax(
			tape = "files/shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectRepresentationName")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectRepresentationName()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		String incorrectRepresentationName = "schema_000101";
		String contentString = "Test_123456789_1";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.uploadFile(cloudId, incorrectRepresentationName, version, contentStream, mediaType, contentChecksum);
	}


	@Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectVersion")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectVersion()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
		String contentString = "Test_123456789_1";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.uploadFile(cloudId, representationName, incorrectVersion, contentStream, mediaType, contentChecksum);
	}


	@Betamax(tape = "files/shouldThrowCannotModifyPersistentRepresentationExceptionForUploadFileWithChecksum")
	@Test(expected = CannotModifyPersistentRepresentationException.class)
	public void shouldThrowCannotModifyPersistentRepresentationExceptionForUploadFileWithChecksum()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		String persistedVersion = "80441ab0-a38d-11e3-8614-50e549e85271";
		String contentString = "Test_123456789_1";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.uploadFile(cloudId, representationName, persistedVersion, contentStream, mediaType, contentChecksum);
	}


	@Betamax(tape = "files/shouldThrowIOExceptionForUploadFile")
	@Test(expected = IOException.class)
	public void shouldThrowIOExceptionForUploadFile()
		throws UnsupportedEncodingException, MCSException, IOException
	{
		String contentString = "Test_123456789_1";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		String incorrectContentChecksum = contentChecksum.substring(1) + "0";
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.uploadFile(cloudId, representationName, version, contentStream, mediaType, incorrectContentChecksum);
	}


	@Betamax(tape = "files/shouldModyfiyFile")
	@Test
	public void shouldModyfiyFile()
		throws UnsupportedEncodingException, IOException, MCSException
	{
		String contentString = "Test_123456789_123456";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		FileServiceClient instance = new FileServiceClient(baseUrl);

		URI uri = instance.modyfiyFile(cloudId, representationName, version, contentStream, mediaType,
			modyfiedFileName, contentChecksum);

		assertNotNull(uri);
		Response response = client.target(uri).request().get();
		InputStream responseStream = response.readEntity(InputStream.class);
		String responseChecksum = response.getEntityTag().toString();
		assertNotNull(responseStream);
		assertEquals("Checksum is incorrect", contentChecksum,
			responseChecksum.subSequence(1, responseChecksum.length() - 1).toString());
		assertArrayEquals("Content is incorrect", contentBytes, ByteStreams.toByteArray(responseStream));
	}


	@Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForModyfiyFileWhenIncorrectCloudId")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsExceptionForModyfiyFileWhenIncorrectCloudId()
		throws UnsupportedEncodingException, IOException, MCSException
	{
		String incorrectCloudId = "12c068c9-461d-484e-878f-099c5fca4400";
		String contentString = "Test_123456789_123456";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.modyfiyFile(incorrectCloudId, representationName, version, contentStream, mediaType, modyfiedFileName,
			contentChecksum);
	}


	@Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForModyfiyFileWhenIncorrectRepresentationName")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsExceptionForModyfiyFileWhenIncorrectRepresentationName()
		throws UnsupportedEncodingException, IOException, MCSException
	{
		String incorrectRepresentationName = "schema_000101";
		String contentString = "Test_123456789_123456";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.modyfiyFile(cloudId, incorrectRepresentationName, version, contentStream, mediaType, modyfiedFileName,
			contentChecksum);
	}


	@Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForModyfiyFileWhenIncorrectVersion")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsExceptionForModyfiyFileWhenIncorrectVersion()
		throws UnsupportedEncodingException, IOException, MCSException
	{
		String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
		String contentString = "Test_123456789_123456";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.modyfiyFile(cloudId, representationName, incorrectVersion, contentStream, mediaType, modyfiedFileName,
			contentChecksum);
	}


	@Betamax(tape = "files/shouldThrowCannotModifyPersistentRepresentationExceptionForModyfiyFileWhenIncorrectVersion")
	@Test(expected = CannotModifyPersistentRepresentationException.class)
	public void shouldThrowCannotModifyPersistentRepresentationExceptionForModyfiyFileWhenIncorrectVersion()
		throws UnsupportedEncodingException, IOException, MCSException
	{
		String persistedVersion = "80441ab0-a38d-11e3-8614-50e549e85271";
		String persistedFileName = "fcec9675-f7e5-4d44-a4a8-0ca12087a2c4";
		String contentString = "Test_123456789_123456";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.modyfiyFile(cloudId, representationName, persistedVersion, contentStream, mediaType,
			persistedFileName, contentChecksum);
	}


	@Betamax(tape = "files/shouldThrowIOExceptionForModyfiyFile")
	@Test(expected = IOException.class)
	public void shouldThrowIOExceptionForModyfiyFile()
		throws UnsupportedEncodingException, IOException, MCSException
	{
		String contentString = "Test_123456789_123456";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		String incorrectContentChecksum = contentChecksum.substring(1) + "0";
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.modyfiyFile(cloudId, representationName, version, contentStream, mediaType, modyfiedFileName,
			incorrectContentChecksum);
	}


	@Betamax(tape = "files/shouldThrowDriverExceptionForModyfiyFile")
	@Test(expected = DriverException.class)
	public void shouldThrowDriverExceptionForModyfiyFile()
		throws UnsupportedEncodingException, IOException, MCSException
	{
		String contentString = "Test_123456789_123456";
		byte[] contentBytes = contentString.getBytes("UTF-8");
		InputStream contentStream = new ByteArrayInputStream(contentBytes);
		String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.modyfiyFile(cloudId, representationName, version, contentStream, mediaType, modyfiedFileName,
			contentChecksum);
	}


	@Betamax(tape = "files/shouldDeleteFile")
	@Test
	public void shouldDeleteFile()
		throws MCSException
	{
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.deleteFile(cloudId, representationName, version, deletedFileName);

		Response response = BuildWebTarget(cloudId, representationName, version, deletedFileName).request().get();
		assertEquals("", Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
	}


	@Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForDeleteFileWhenIncorrectCloudId")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsExceptionForDeleteFileWhenIncorrectCloudId()
		throws MCSException
	{
		String incorrectCloudId = "7MZWQJF8P99";
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.deleteFile(incorrectCloudId, representationName, version, deletedFileName);
	}


	@Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForDeleteFileWhenIncorrectRepresentationName")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsExceptionForDeleteFileWhenIncorrectRepresentationName()
		throws MCSException
	{
		String incorrectRepresentationName = "schema_000101";
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.deleteFile(cloudId, incorrectRepresentationName, version, deletedFileName);
	}


	@Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForDeleteFileWhenIncorrectVersion")
	@Test(expected = RepresentationNotExistsException.class)
	public void shouldThrowRepresentationNotExistsExceptionForDeleteFileWhenIncorrectVersion()
		throws MCSException
	{
		String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.deleteFile(cloudId, representationName, incorrectVersion, deletedFileName);
	}


	@Betamax(tape = "files/shouldThrowFileNotExistsExceptionForDeleteFile")
	@Test(expected = FileNotExistsException.class)
	public void shouldThrowFileNotExistsExceptionForDeleteFile()
		throws MCSException
	{
		String notExistDeletedFileName = "d64b423b-1018-4526-ab4b-3539261ff000";
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.deleteFile(cloudId, representationName, version, notExistDeletedFileName);
	}


	@Betamax(tape = "files/shouldThrowCannotModifyPersistentRepresentationExceptionForDeleteFile")
	@Test(expected = CannotModifyPersistentRepresentationException.class)
	public void shouldThrowCannotModifyPersistentRepresentationExceptionForDeleteFile()
		throws MCSException
	{
		String persistedVersion = "80441ab0-a38d-11e3-8614-50e549e85271";
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.deleteFile(cloudId, representationName, persistedVersion, deletedFileName);
	}


	@Betamax(tape = "files/shouldThrowDriverExceptionForDeleteFile")
	@Test(expected = DriverException.class)
	public void shouldThrowDriverExceptionForDeleteFile()
		throws MCSException
	{
		FileServiceClient instance = new FileServiceClient(baseUrl);

		instance.deleteFile(cloudId, representationName, version, deletedFileName);
	}


	private static WebTarget BuildWebTarget(String cloudId, String schema, String version, String fileName)
	{
		return client.target(baseUrl).path(filePath).resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
				.resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, schema)
				.resolveTemplate(ParamConstants.P_VER, version).resolveTemplate(ParamConstants.P_FILENAME, fileName);
	}
}
