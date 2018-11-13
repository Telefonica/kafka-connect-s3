package com.spredfast.kafka.connect.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.util.Map;
import java.util.Objects;

public class S3 {

	public static AmazonS3 s3client(Map<String, String> config) {
		ClientConfiguration clientConfiguration = new ClientConfiguration();

		AmazonS3ClientBuilder s3Client = AmazonS3ClientBuilder
			.standard()
			.withClientConfiguration(clientConfiguration)
			// Use default credentials provider that looks in Env + Java properties + profile + instance role
			.withCredentials(new DefaultAWSCredentialsProviderChain());

		// If worker config sets explicit endpoint override (e.g. for testing) use that
		String s3Endpoint = config.get("s3.endpoint");
		if (s3Endpoint != null && !Objects.equals(s3Endpoint, "")) {
			s3Client.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3Endpoint, s3Client.getRegion()));
		}

		Boolean s3PathStyle = Boolean.parseBoolean(config.get("s3.path_style"));
		if (s3PathStyle) {
			s3Client.withPathStyleAccessEnabled(true);
		}

		return s3Client.build();
	}
}
