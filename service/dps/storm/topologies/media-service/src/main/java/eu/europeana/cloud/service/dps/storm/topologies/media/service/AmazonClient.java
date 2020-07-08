package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import lombok.AllArgsConstructor;
import lombok.Builder;

import javax.annotation.PostConstruct;
import java.io.InputStream;
import java.io.Serializable;

@Builder
@AllArgsConstructor
public class AmazonClient implements Serializable {
	static AmazonS3 amazonClient;
	private final String awsAccessKey;
	private final String awsSecretKey;
	private final String awsEndPoint;
	private final String awsBucket;

	@PostConstruct
	synchronized void init() {
		if (amazonClient == null) {
			amazonClient = new AmazonS3Client(new BasicAWSCredentials(
					awsAccessKey,
					awsSecretKey));
			amazonClient.setEndpoint(awsEndPoint);
		}
	}

	/**
	 * Store object in the specified bucket
	 * @param bucket bucket name
	 * @param name object name
	 * @param inputStream object content
	 * @param objectMetadata object metadata
	 * @return result from AmazonS3
	 */
	PutObjectResult putObject(String bucket, String name, InputStream inputStream, ObjectMetadata objectMetadata) {
		return amazonClient.putObject(bucket, name, inputStream, objectMetadata);
	}

	/**
	 * Store object in the default bucket specified during creation of the client
	 * @param name object name
	 * @param inputStream object content
	 * @param objectMetadata object metadata
	 * @return result from AmazonS3
	 */
	PutObjectResult putObject(String name, InputStream inputStream, ObjectMetadata objectMetadata) {
		return amazonClient.putObject(awsBucket, name, inputStream, objectMetadata);
	}
}
