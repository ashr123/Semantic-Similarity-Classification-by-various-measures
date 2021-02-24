package il.co.dsp211.assignment3.steps.utils;

import com.amazonaws.util.StringInputStream;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.Iterator;

public class HelperMethods
{
	public static byte[] objectToBytes(final Object obj) throws IOException
	{
		try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		     final ObjectOutputStream oos = new ObjectOutputStream(bos))
		{
			oos.writeObject(obj);
			return bos.toByteArray();
		}
	}

	public static Serializable bytesToObject(final byte[] data) throws ClassNotFoundException, IOException
	{
		try (final ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(data)))
		{
			return (Serializable) objectInputStream.readObject();
		}
	}

	public static void downloadFileFromS3Bucket(S3Client s3Client, String bucketName, String key/*, String outputPathString*/)
	{
		System.out.println("Getting object " + key + " and saving it to ./" + key/*outputPathString*/ + " ...");

		s3Client.getObject(GetObjectRequest.builder()
						.bucket(bucketName)
						.key(key)
						.build(),
				pathOf(key/*outputPathString*/));

		System.out.println("Object downloaded and saved");
	}

	public static Path pathOf(String first, String... more)
	{
		return FileSystems.getDefault().getPath(first, more);
	}

	public static SequenceInputStream bucketBatch(S3Client s3Client, String bucketName, String folderPrefix) throws UnsupportedEncodingException
	{
		// To delete a bucket, all the objects in the bucket must be deleted first
		ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
				.bucket(bucketName)
				.prefix(folderPrefix)
				.build();
		ListObjectsV2Response listObjectsV2Response;

		SequenceInputStream sequenceInputStream = null;
		do
		{
			listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
			if (!listObjectsV2Response.contents().isEmpty())
			{
				ListObjectsV2Response finalListObjectsV2Response = listObjectsV2Response;
				sequenceInputStream = new SequenceInputStream(sequenceInputStream == null ? new StringInputStream(
						"@relation \"Word Relatedness\"\n" +
						"\n" +
						"@attribute freq_distManhattan real\n" +
						"@attribute freq_distEuclidean real\n" +
						"@attribute freq_simCosine real\n" +
						"@attribute freq_simJaccard real\n" +
						"@attribute freq_simDice real\n" +
						"@attribute freq_simJS real\n" +
						"\n" +
						"@attribute prob_distManhattan real\n" +
						"@attribute prob_distEuclidean real\n" +
						"@attribute prob_simCosine real\n" +
						"@attribute prob_simJaccard real\n" +
						"@attribute prob_simDice real\n" +
						"@attribute prob_simJS real\n" +
						"\n" +
						"@attribute PMI_distManhattan real\n" +
						"@attribute PMI_distEuclidean real\n" +
						"@attribute PMI_simCosine real\n" +
						"@attribute PMI_simJaccard real\n" +
						"@attribute PMI_simDice real\n" +
						"@attribute PMI_simJS real\n" +
						"\n" +
						"@attribute ttest_distManhattan real\n" +
						"@attribute ttest_distEuclidean real\n" +
						"@attribute ttest_simCosine real\n" +
						"@attribute ttest_simJaccard real\n" +
						"@attribute ttest_simDice real\n" +
						"@attribute ttest_simJS real\n" +
						"\n" +
						"@attribute class {true, false}\n" +
						"\n" +
						"@data\n") : sequenceInputStream,
						new SequenceInputStream(new Enumeration<ResponseInputStream<GetObjectResponse>>()
						{
							private final Iterator<ResponseInputStream<GetObjectResponse>> iterator = finalListObjectsV2Response.contents().stream()
									.map(S3Object::key)
									.filter(key -> key.contains("part-r-"))
									.peek(key -> System.out.println("Collecting file: " + key))
									.map(key -> s3Client.getObject(GetObjectRequest.builder()
											.bucket(bucketName)
											.key(key)
											.build()))
									.iterator();

							@Override
							public boolean hasMoreElements()
							{
								return iterator.hasNext();
							}

							@Override
							public ResponseInputStream<GetObjectResponse> nextElement()
							{
								return iterator.next();
							}
						}));

				listObjectsV2Request = ListObjectsV2Request.builder()
						.bucket(bucketName)
						.continuationToken(listObjectsV2Response.nextContinuationToken())
						.build();
			}
		} while (listObjectsV2Response.isTruncated());
		return sequenceInputStream;
	}
}
