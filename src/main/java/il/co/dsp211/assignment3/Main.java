package il.co.dsp211.assignment3;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import il.co.dsp211.assignment3.steps.step1.EMR;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class Main
{
	static
	{
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);
	}

	public static void main(String... args) throws IOException
	{
		System.out.println("Creating cluster...");
		final Properties properties = new Properties();
		try (InputStream input = new FileInputStream("config.properties"))
		{
			properties.load(input);
		}

		// create an EMR client using the credentials and region specified in order to create the cluster
		System.out.println("Cluster created with ID: " + AmazonElasticMapReduceClientBuilder.standard()
				.withRegion(Regions.valueOf(properties.getProperty("region").toUpperCase()))
				.build()
				// create the cluster
				.runJobFlow(new RunJobFlowRequest()
						.withName("Semantic Similarity Classification by various measures")
						.withReleaseLabel("emr-6.2.0") // specifies the EMR release version label, we recommend the latest release
						// create a step to enable debugging in the AWS Management Console
						.withSteps(new StepConfig("EMR", new HadoopJarStepConfig("s3://" + properties.getProperty("bucketName") + "/" + properties.getProperty("jarFileName") + ".jar")
//								.withMainClass(EMR.class.getName()) // TODO: ???
								.withArgs("s3://" + properties.getProperty("bucketName") + "/",
										properties.getProperty("isReadSubset"),
										properties.getProperty("goldenStandardFileName"),
										properties.getProperty("numOfFeaturesToSkip"),
										properties.getProperty("numOfFeatures"))))
						.withLogUri("s3://" + properties.getProperty("bucketName") + "/logs") // a URI in S3 for log files is required when debugging is enabled
						.withServiceRole("EMR_DefaultRole") // replace the default with a custom IAM service role if one is used
						.withJobFlowRole("EMR_EC2_DefaultRole") // replace the default with a custom EMR role for the EC2 instance profile if one is used
						.withInstances(new JobFlowInstancesConfig()
								.withInstanceCount(Integer.parseInt(properties.getProperty("instanceCount")))
								.withKeepJobFlowAliveWhenNoSteps(false)
								.withMasterInstanceType(InstanceType.C5Xlarge.toString())
								.withSlaveInstanceType(InstanceType.C5Xlarge.toString())))
				.getJobFlowId());
	}
}
