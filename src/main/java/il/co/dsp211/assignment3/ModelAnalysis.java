package il.co.dsp211.assignment3;

import il.co.dsp211.assignment3.steps.step1.ModelClassifier;
import il.co.dsp211.assignment3.steps.utils.HelperMethods;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import weka.classifiers.Classifier;
import weka.core.DenseInstance;
import weka.core.SerializationHelper;

import java.io.*;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.IntStream;

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
		     BufferedReader arff = new BufferedReader(new InputStreamReader(loadData(s3Client, properties.getProperty("bucketName"), "Step4Output-BuildDistancesVectors/")));
		     BufferedWriter writer = new BufferedWriter(new FileWriter("MLP_model_Full_Analysis.txt", true)))
		{
			arff.lines().forEach(new Consumer<String>()
			{
				final ModelClassifier modelClassifier = new ModelClassifier();
				final Classifier cls = (Classifier) SerializationHelper.read("model.bin");
				final double[] vector24D = new double[25];
				final DenseInstance DENSE_INSTANCE = new DenseInstance(1.0, vector24D);
				boolean isEven = false;
				String wordPair;

				@Override
				public void accept(String line)
				{
					if (!isEven)
					{
						wordPair = line;
					} else
					{
						final String[] split = line.split("[,\t]");
						IntStream.range(0, split.length - 1).parallel()
								.forEach(i -> vector24D[i] = split[i].equals("?") ? Double.NaN : Double.parseDouble(split[i]));
						final boolean isWordPairSimilar = Boolean.parseBoolean(split[split.length - 1]);
						vector24D[24] = isWordPairSimilar ? 0 : 1;
						try
						{
							final boolean wordPairResultPrediction = Boolean.parseBoolean(modelClassifier.classify(cls, DENSE_INSTANCE));


						// Uncomment for stdout prints
//						if (isWordPairSimilar && !wordPairResultPrediction)
//							System.out.println("False Negative:\t" + wordPair);
//						else if (!isWordPairSimilar && wordPairResultPrediction)
//							System.out.println("False Positive:\t" + wordPair);
//						else if (isWordPairSimilar && wordPairResultPrediction)
//							System.out.println("True Positive:\t" + wordPair);
//						else
//							System.out.println("True Negative:\t" + wordPair);

						if (isWordPairSimilar && !wordPairResultPrediction)
								writer.write("False Negative:\t" + wordPair + "\n");
							else if (!isWordPairSimilar && wordPairResultPrediction)
								writer.write("False Positive:\t" + wordPair + "\n");
							else if (isWordPairSimilar && wordPairResultPrediction)
								writer.write("True Positive:\t" + wordPair + "\n");
							else
								writer.write("True Negative:\t" + wordPair + "\n");
						}
						catch (Exception e)
						{
							e.printStackTrace();
						}
					}
					isEven = !isEven;
				}
			});
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static SequenceInputStream loadData(S3Client s3Client, String bucketName, String folderPrefix)
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
				final Iterator<ResponseInputStream<GetObjectResponse>> iterator = listObjectsV2Response.contents().stream()
						.map(S3Object::key)
						.filter(key -> key.contains("part-r-"))
						.peek(key -> System.out.println("Collecting file: " + key))
						.map(key -> s3Client.getObject(GetObjectRequest.builder()
								.bucket(bucketName)
								.key(key)
								.build()))
						.iterator();
				final SequenceInputStream sequence = new SequenceInputStream(new Enumeration<ResponseInputStream<GetObjectResponse>>()
				{
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
				sequenceInputStream = sequenceInputStream == null ? sequence : new SequenceInputStream(sequenceInputStream, sequence);

				listObjectsV2Request = ListObjectsV2Request.builder()
						.bucket(bucketName)
						.continuationToken(listObjectsV2Response.nextContinuationToken())
						.build();
			}
		} while (listObjectsV2Response.isTruncated());
		return sequenceInputStream;
	}
}
