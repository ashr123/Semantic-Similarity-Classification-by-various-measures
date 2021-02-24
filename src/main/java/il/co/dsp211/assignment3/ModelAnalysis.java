package il.co.dsp211.assignment3;

import il.co.dsp211.assignment3.steps.step1.ModelClassifier;
import il.co.dsp211.assignment3.steps.utils.HelperMethods;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.*;
import java.nio.file.Files;
import java.util.Properties;
import java.util.function.Consumer;

import static il.co.dsp211.assignment3.steps.step1.EMR.bucketBatch;

public class ModelAnalysis {

	static {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {

		final Properties properties = new Properties();
		try (InputStream input = new FileInputStream("config.properties")) {
			properties.load(input);
		}

		if (!Files.exists(HelperMethods.pathOf("model.bin"))) {
			try (S3Client s3Client = S3Client.builder()
					.region(Region.of(properties.getProperty("region").toLowerCase().replace('_', '-')))
					.build();
			) {
				HelperMethods.downloadFileFromS3Bucket(s3Client, properties.getProperty("bucketName"), "model.bin");
			}
		}

		//classifiy a single instance
		ModelClassifier cls = new ModelClassifier();

		try (S3Client s3Client = S3Client.builder()
				.region(Region.of(properties.getProperty("region").toLowerCase().replace('_', '-')))
				.build();
		     BufferedReader arff = new BufferedReader(new InputStreamReader(bucketBatch(s3Client, properties.getProperty("bucketName"), "Step4Output-BuildDistancesVectors/")))) {
			arff.lines().forEach(new Consumer<String>() {
				final double[] vector24D = new double[25];
				boolean isEven = false;
				String wordPair = "";
				boolean isWordPairSimilar;

				@Override
				public void accept(String line) {
					if (!isEven) {
						wordPair = line;
					} else {
						String[] split = line.split("[,\t]");
						for (int i = 0; i < split.length - 1; i++) {
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
}
