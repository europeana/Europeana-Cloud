package eu.europeana.cloud.test;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import io.findify.s3mock.S3Mock;

import static eu.europeana.cloud.test.S3TestHelper.S3TestConstants.*;

public class S3TestHelper {

        private static S3Mock api;
        private static AmazonS3Client client;
        public static void setUpTest(){
                api = new S3Mock.Builder()
                        .withInMemoryBackend()
                        .withPort(S3_TEST_PORT)
                        .build();
                api.start();
                client = new AmazonS3Client(new AnonymousAWSCredentials());
                client.setEndpoint(S3_TEST_ENDPOINT);
                client.createBucket(S3_TEST_CONTAINER);
        }

        public static void cleanupAfterTest(){
                client.deleteBucket(S3_TEST_CONTAINER);
                client.createBucket(S3_TEST_CONTAINER);
        }

        public static void cleanupAfterTests(){
                api.stop();
        }

        public class S3TestConstants {
                public static final Integer S3_TEST_PORT = 8001;
                public static final String S3_TEST_ENDPOINT = "http://127.0.0.1:" + S3_TEST_PORT;
                public static final String S3_TEST_CONTAINER = "test-container";
                public static final String S3_TEST_USER = "test_user";
                public static final String S3_TEST_PASSWORD = "test_pwd";
                public static final String S3_TEST_REGION = "test_region";
                private S3TestConstants() {
                }
        }
}
