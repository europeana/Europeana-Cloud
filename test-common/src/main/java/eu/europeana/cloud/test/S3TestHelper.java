package eu.europeana.cloud.test;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;

import java.util.concurrent.ThreadLocalRandom;

import static eu.europeana.cloud.test.S3TestHelper.S3TestConstants.*;

/**
 * S3 test helper class that is meant to start S3 in memory mock and provide some constants to be used in tests to connect to said mock.
 */
public final class S3TestHelper {
        private static S3Mock api;
        private static AmazonS3Client client;
        private S3TestHelper() {}
        /**
         * Start S3 mock in memory backend on randomly generated port and creates test bucket in it
         */
        public static void startS3MockServer(){
                api = new S3Mock.Builder()
                        .withInMemoryBackend()
                        .withPort(S3_TEST_PORT)
                        .build();
                api.start();

                client = (AmazonS3Client) AmazonS3ClientBuilder
                        .standard()
                        .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(S3_TEST_ENDPOINT, S3_TEST_REGION))
                        .build();
                client.createBucket(S3_TEST_CONTAINER);
        }

        /**
         * Cleans up after a single test by deleting and recreating the S3 test bucket.
         * This ensures that the bucket is empty and ready for the next test.
         */
        public static void cleanUpBetweenTests(){
                client.deleteBucket(S3_TEST_CONTAINER);
                client.createBucket(S3_TEST_CONTAINER);
        }

        /**
         * Stops the S3 mock server after all tests have been executed.
         */
        public static void stopS3MockServer() {
            api.stop();
        }

        /**
         * A class that contains constants used in S3 tests.
         */
        public static final class S3TestConstants {
                public static final Integer S3_TEST_PORT = ThreadLocalRandom.current().nextInt(10000, 65536);
                public static final String S3_TEST_ENDPOINT = "http://127.0.0.1:" + S3_TEST_PORT;
                public static final String S3_TEST_CONTAINER = "test-container";
                public static final String S3_TEST_USER = "test_user";
                public static final String S3_TEST_PASSWORD = "test_pwd";
                public static final String S3_TEST_REGION = "test_region";
                private S3TestConstants() {
                }
        }
}
