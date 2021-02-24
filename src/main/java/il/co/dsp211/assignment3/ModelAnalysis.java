package il.co.dsp211.assignment3;

import il.co.dsp211.assignment3.steps.step1.ModelClassifier;
import il.co.dsp211.assignment3.steps.utils.HelperMethods;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Properties;

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

		if (!Files.exists(HelperMethods.pathOf("model.bin")))
			try (S3Client s3Client = S3Client.builder()
					.region(Region.of(properties.getProperty("region").toLowerCase().replace('_', '-')))
					.build();
			) {
				HelperMethods.downloadFileFromS3Bucket(s3Client, properties.getProperty("bucketName"), "model.bin");
			}

		//classifiy a single instance
		ModelClassifier cls = new ModelClassifier();
		String classname = cls.classifiy(cls.createInstance(49.0, Double.NaN, Double.NaN, 0.11954765751211632, 0.21203438395415472, Double.NaN, 0.01342947658337052, 0.003050110981127867, 0.20497174173280414, 0.08748953264070679, 0.16144789226761383, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, 0.019105725857922466, 0.0012918267776457278, 0.2835473161540472, 5.613983846724007, 1.6783791937585641, Double.NaN, 0), "model.bin");

		System.out.println("<restaur, abund> is " + classname);
	}
}
