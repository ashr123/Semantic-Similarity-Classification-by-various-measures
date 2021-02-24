package il.co.dsp211.assignment3;

import com.amazonaws.util.StringInputStream;
import il.co.dsp211.assignment3.steps.step1.ModelClassifier;
import il.co.dsp211.assignment3.steps.utils.HelperMethods;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.function.Consumer;

public class ModelAnalysis
{
	static
	{
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException
	{
		final Properties properties = new Properties();
		try (InputStream input = new FileInputStream("config.properties"))
		{
			properties.load(input);
		}

		if (!Files.exists(HelperMethods.pathOf("model.bin")))
		{
			try (S3Client s3Client = S3Client.builder()
					.region(Region.of(properties.getProperty("region").toLowerCase().replace('_', '-')))
					.build())
			{
				HelperMethods.downloadFileFromS3Bucket(s3Client, properties.getProperty("bucketName"), "model.bin");
			}
		}

		try (S3Client s3Client = S3Client.builder()
				.region(Region.of(properties.getProperty("region").toLowerCase().replace('_', '-')))
				.build();
		     BufferedReader arff = new BufferedReader(new InputStreamReader(loadData(s3Client, properties.getProperty("bucketName"), "Step4Output-BuildDistancesVectors/"))))
		{
			arff.lines().forEach(new Consumer<String>()
			{
				//classifiy a single instance
				final ModelClassifier cls = new ModelClassifier();
				final double[] vector24D = new double[25];
				boolean isEven = false;
				String wordPair;
				boolean isWordPairSimilar;

				@Override
				public void accept(String line)
				{
					if (line.isEmpty())
					{
						System.out.println("Line is empty!");
						return;
					}
					if (!isEven)
					{
						wordPair = line;
					} else
					{
						String[] split = line.split("[,\t]");
						for (int i = 0; i < split.length - 1; i++)
						{
							vector24D[i] = split[i].equals("?") ? Double.NaN : Double.parseDouble(split[i]);
						}
						vector24D[vector24D.length - 1] = 0;
						isWordPairSimilar = Boolean.parseBoolean(split[split.length - 1]);

						String classname = cls.classifiy(cls.createInstance(vector24D), "model.bin");
						boolean wordPairResultPrediction = Boolean.parseBoolean(classname);

						if (isWordPairSimilar == true && wordPairResultPrediction == false)
							System.out.println("False Negative:" + wordPair);
						else if (isWordPairSimilar == false && wordPairResultPrediction == true)
							System.out.println("False Positive:" + wordPair);
					}
					isEven = !isEven;
				}
			});
		}
	}

	public static SequenceInputStream loadData(S3Client s3Client, String bucketName, String folderPrefix) throws UnsupportedEncodingException
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
				final SequenceInputStream sequence = new SequenceInputStream(new Enumeration<ResponseInputStream<GetObjectResponse>>()
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
				});
				sequenceInputStream = sequenceInputStream == null ? sequence : new SequenceInputStream( sequenceInputStream, sequence);

				listObjectsV2Request = ListObjectsV2Request.builder()
						.bucket(bucketName)
						.continuationToken(listObjectsV2Response.nextContinuationToken())
						.build();
			}
		} while (listObjectsV2Response.isTruncated());
		return sequenceInputStream;
	}
}
