package il.co.dsp211.assignment3;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class Main
{
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
						.withName("Knowledge base for Word Predictor")
						.withReleaseLabel("emr-6.2.0") // specifies the EMR release version label, we recommend the latest release
						// create a step to enable debugging in the AWS Management Console
						.withSteps(new StepConfig("EMR with combiners", new HadoopJarStepConfig("s3://" + properties.getProperty("bucketName") + "/" + properties.getProperty("jarFileName") + ".jar")
								.withArgs("s3://" + properties.getProperty("bucketName") + "/",
										Boolean.toString(true),
										properties.getProperty("singleLetterInAWordRegex"),
										properties.getProperty("wordsBucket"))))
						.withLogUri("s3://" + properties.getProperty("bucketName") + "/logs") // a URI in S3 for log files is required when debugging is enabled
						.withServiceRole("EMR_DefaultRole") // replace the default with a custom IAM service role if one is used
						.withJobFlowRole("EMR_EC2_DefaultRole") // replace the default with a custom EMR role for the EC2 instance profile if one is used
						.withInstances(new JobFlowInstancesConfig()
								.withInstanceCount(Integer.parseInt(properties.getProperty("instanceCount")))
								.withKeepJobFlowAliveWhenNoSteps(false)
								.withMasterInstanceType(InstanceType.M5Xlarge.toString())
								.withSlaveInstanceType(InstanceType.M5Xlarge.toString())))
				.getJobFlowId());
	}
}
