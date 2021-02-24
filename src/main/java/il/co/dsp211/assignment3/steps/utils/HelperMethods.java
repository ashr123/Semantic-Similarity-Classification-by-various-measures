package il.co.dsp211.assignment3.steps.utils;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;

public class HelperMethods {

	public static byte[] objectToBytes(final Object obj) throws IOException {
		try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		     final ObjectOutputStream oos = new ObjectOutputStream(bos)) {
			oos.writeObject(obj);
			return bos.toByteArray();
		}
	}

	public static Serializable bytesToObject(final byte[] data) throws ClassNotFoundException, IOException {
		try (final ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(data))) {
			return (Serializable) objectInputStream.readObject();
		}
	}

	public static void downloadFileFromS3Bucket(S3Client s3Client, String bucketName, String key/*, String outputPathString*/) {
		System.out.println("Getting object " + key + " and saving it to ./" + key/*outputPathString*/ + " ...");

		s3Client.getObject(GetObjectRequest.builder()
						.bucket(bucketName)
						.key(key)
						.build(),
				pathOf(key/*outputPathString*/));

		System.out.println("Object downloaded and saved");
	}

	public static Path pathOf(String first, String... more) {
		return FileSystems.getDefault().getPath(first, more);
	}

}
