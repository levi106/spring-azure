package com.contoso.frontapi;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@SpringBootApplication
public class FrontApplication {
	private final static String STORAGE_ACCOUNT_NAME = System.getenv("STORAGE_ACCOUNT_NAME");
	private final static String STORAGE_CONTAINER_NAME = System.getenv("STORAGE_CONTAINER_NAME");

	private final Logger logger = LoggerFactory.getLogger(FrontApplication.class);
	private final String endpoint = String.format(Locale.ROOT, "https://%s.blob.core.windows.net", STORAGE_ACCOUNT_NAME);
	private final SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd_HHmmss");

	public FrontApplication() {
	}

	@RequestMapping(value = "/health", method = RequestMethod.GET)
	String health(@RequestHeader Map<String,String> headers) {
		headers.forEach((key, value) -> {
			logger.debug(String.format("Header '%s' = %s", key, value));
		});
		return "Ok";
	}

	@RequestMapping(value = "/upload", method = RequestMethod.POST)
	String upload(@RequestHeader Map<String,String> headers, @RequestBody final String body) {
		headers.forEach((key, value) -> {
			logger.debug(String.format("Header '%s' = %s", key, value));
		});
		Date now = Date.from(Instant.now());
		String blobName = String.format("%s.json", df.format(now));
		try {
			BlobServiceClient storageClient = new BlobServiceClientBuilder()
				.endpoint(this.endpoint)
				.credential(new DefaultAzureCredentialBuilder().build())
				.buildClient();
			BlobContainerClient blobContainerClient = storageClient.getBlobContainerClient(STORAGE_CONTAINER_NAME);
			if (!blobContainerClient.exists()) {
				logger.debug("Create a container (%s)", STORAGE_CONTAINER_NAME);
				blobContainerClient.create();
			}
			BlockBlobClient blobClient = blobContainerClient
				.getBlobClient(blobName)
				.getBlockBlobClient();
			String data = String.format("{ \"message\": \"%s\" }", body);
			try {
				InputStream dataStream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
				blobClient.upload(dataStream, data.length());
				dataStream.close();
			} catch (IOException ex) {
				logger.error("IOException: failed to upload file %s", ex.getMessage());
			}
		} catch (BlobStorageException ex) {
			if (ex.getErrorCode() == BlobErrorCode.RESOURCE_NOT_FOUND) {
				logger.error("BlobStorageException: RESOURCE_NOT_FOUND %s", ex.getMessage());
			} else if (ex.getErrorCode() == BlobErrorCode.CONTAINER_BEING_DELETED) {
				logger.error("BlobStorageException: CONTAINER_BEING_DELETED %s", ex.getMessage());
			} else if (ex.getErrorCode() == BlobErrorCode.CONTAINER_ALREADY_EXISTS) {
				logger.error("BlobStorageException: CONTAINER_ALREADY_EXISTS %s", ex.getMessage());
			}
		}

		return blobName;
	}

	public static void main(String[] args) {
		SpringApplication.run(FrontApplication.class, args);
	}

}
