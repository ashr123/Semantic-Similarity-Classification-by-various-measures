package il.co.dsp211.assignment3.steps.step1;

import com.amazonaws.regions.Regions;
import com.amazonaws.util.StringInputStream;
import il.co.dsp211.assignment3.steps.step1.jobs.BuildCoVectors;
import il.co.dsp211.assignment3.steps.step1.jobs.BuildDistancesVectors;
import il.co.dsp211.assignment3.steps.step1.jobs.CorpusPairFilter;
import il.co.dsp211.assignment3.steps.step1.jobs.CorpusWordCount;
import il.co.dsp211.assignment3.steps.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.Iterator;

public class EMR
{
	public static void main(String... args) throws Exception
	{
		boolean jobStatus;
		final Configuration conf = new Configuration();

		// TODO: TESTING
//		long milliSeconds = 1000*60*60; //  default is 600000, likewise can give any value)
//		conf.setLong("mapred.task.timeout", milliSeconds);

		System.out.println("Building job 1 - Corpus Word Count...");

		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(CorpusWordCount.class);

		job1.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		job1.setMapperClass(CorpusWordCount.WordCounterMapper.class);
		job1.setMapOutputKeyClass(StringStringPair.class);
		job1.setMapOutputValueClass(LongWritable.class);

		job1.setCombinerClass(CorpusWordCount.PairSummerCombinerAndReducer.class);

		job1.setReducerClass(CorpusWordCount.PairSummerCombinerAndReducer.class);
		job1.setOutputKeyClass(StringStringPair.class);
		job1.setOutputValueClass(LongWritable.class);

		final Path corpusPath = new Path("s3://assignment3dsp/biarcs/biarcs.00-of-99");
		FileInputFormat.addInputPath(job1, corpusPath);
		FileOutputFormat.setOutputPath(job1, new Path(args[0] + "Step1Output-CorpusWordCount"));

		System.out.println("Done building!\n" +
		                   "Starting job 1 - Corpus Word Count...");
		System.out.println("Job 1 - Corpus Word Count: completed with success status: " + (jobStatus = job1.waitForCompletion(true)) + "!");

		if (!jobStatus)
			return;

		//--------------------------------------------------------------------------------------------------------------

		System.out.println("Building job 2 - CorpusPairFilter...");

		conf.setLong("CounterFL", job1.getCounters().findCounter(NCounter.N_COUNTER).getValue());
		conf.setInt("numOfFeaturesToSkip", Integer.parseInt(args[3]));
		conf.setInt("numOfFeatures", Integer.parseInt(args[4]));

		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(CorpusPairFilter.class);

		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);

		job2.setMapperClass(CorpusPairFilter.CastlerMapper.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(StringStringPair.class);

		job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		job2.setReducerClass(CorpusPairFilter.FilterTopPairsReducer.class);
//		job2.setOutputKeyClass(Void.class);
//		job2.setOutputValueClass(Void.class);

		job2.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job2, new Path(args[0] + "Step1Output-CorpusWordCount"));
		FileOutputFormat.setOutputPath(job2, new Path(args[0] + "Step2Output-CorpusPairFilter"));

		System.out.println("Done building!\n" +
		                   "Starting job 2 - CorpusPairFilter...");
		System.out.println("Job 2 - CorpusPairFilter: completed with success status: " + (jobStatus = job2.waitForCompletion(true)) + "!");
		if (!jobStatus)
			return;

		//--------------------------------------------------------------------------------------------------------------

		System.out.println("Building job 3 - BuildCoVectors...");
		conf.set("goldenStandardFileName", args[2]);

		Job job3 = Job.getInstance(conf);
		job3.setJarByClass(BuildCoVectors.class);

		job3.setInputFormatClass(SequenceFileInputFormat.class);
		job3.setOutputFormatClass(SequenceFileOutputFormat.class);

		MultipleInputs.addInputPath(job3, corpusPath, SequenceFileInputFormat.class, BuildCoVectors.VectorRecordFilterMapper.class);
		MultipleInputs.addInputPath(job3, new Path(args[0] + "Step1Output-CorpusWordCount"), SequenceFileInputFormat.class, BuildCoVectors.CounterLittleLMapper.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(StringStringPair.class);

		job3.setReducerClass(BuildCoVectors.CalculateEmbeddingsReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(VectorsQuadruple.class);


		FileOutputFormat.setOutputPath(job3, new Path(args[0] + "Step3Output-BuildCoVectors"));

		System.out.println("Done building!\n" +
		                   "Starting job 3 - BuildCoVectors...");
		System.out.println("Job 3 - BuildCoVectors: completed with success status: " + (jobStatus = job3.waitForCompletion(true)) + "!");
		if (!jobStatus)
			return;

		//--------------------------------------------------------------------------------------------------------------

		System.out.println("Building job 4 - BuildDistancesVectors...");

		Job job4 = Job.getInstance(conf);
		job4.setJarByClass(BuildDistancesVectors.class);

		job4.setInputFormatClass(SequenceFileInputFormat.class);
//		job4.setOutputFormatClass(SequenceFileOutputFormat.class); //TODO: UnComment !!!

		job4.setMapperClass(BuildDistancesVectors.BuildMatchingCoVectorsMapper.class);
		job4.setMapOutputKeyClass(StringBooleanPair.class);
		job4.setMapOutputValueClass(StringVectorsQuadruplePair.class);

		job4.setPartitionerClass(BuildDistancesVectors.JoinPartitioner.class);

		job4.setReducerClass(BuildDistancesVectors.CreatePairDistancesVectorReducer.class);
		job4.setOutputKeyClass(ArrayWritable.class);
		job4.setOutputValueClass(BooleanWritable.class);

		FileInputFormat.addInputPath(job4, new Path(args[0] + "Step3Output-BuildCoVectors"));
		FileOutputFormat.setOutputPath(job4, new Path(args[0] + "Step4Output-BuildDistancesVectors"));

		System.out.println("Done building!\n" +
		                   "Starting job 4 - BuildDistancesVectors...");
		System.out.println("Job 4 - BuildDistancesVectors: completed with success status: " + (jobStatus = job4.waitForCompletion(true)) + "!");
		if (!jobStatus)
			return;

		//--------------------------------------------------------------------------------------------------------------
		// Generate Model

		// TODO: FIX PATHS
		/*
		final String DATASETPATH = "/Users/Emaraic/Temp/ml/iris.2D.arff";
		final String MODElPATH = "/Users/Emaraic/Temp/ml/model.bin";

		ModelGenerator mg = new ModelGenerator();
		 */

		try (S3Client s3Client = S3Client.builder()
				     .region(Region.of(args[5]))
				     .build();
		     BufferedReader arff = new BufferedReader(new InputStreamReader(bucketBatch(s3Client, args[0].substring(5), "Step4Output-BuildDistancesVectors/"))))
		{

			arff.lines().forEach(System.out::println);

			/*
			Instances dataset = mg.loadDataset(arff);

			Filter filter = new Normalize();

			// divide dataset to train dataset 80% and test dataset 20%
			int trainSize = (int) Math.round(dataset.numInstances() * 0.8);
			int testSize = dataset.numInstances() - trainSize;

			dataset.randomize(new Debug.Random(1));// if you comment this line the accuracy of the model will be droped from 96.6% to 80%

			//Normalize dataset
			filter.setInputFormat(dataset);
			Instances datasetnor = Filter.useFilter(dataset, filter);

			Instances traindataset = new Instances(datasetnor, 0, trainSize);
			Instances testdataset = new Instances(datasetnor, trainSize, testSize);

			// build classifier with train dataset
			MultilayerPerceptron ann = (MultilayerPerceptron) mg.buildClassifier(traindataset);

			// Evaluate classifier with test dataset
			String evalsummary = mg.evaluateModel(ann, traindataset, testdataset);
			System.out.println("Evaluation: " + evalsummary);

			//Save model
			mg.saveModel(ann, MODElPATH);
			 */

			//classifiy a single instance
			/*
			TODO: REVIEW
			ModelClassifier cls = new ModelClassifier();
			String classname =cls.classifiy(Filter.useFilter(cls.createInstance(1.6, 0.2, 0), filter), MODElPATH);
			System.out.println("\n The class name for the instance with petallength = 1.6 and petalwidth =0.2 is  " +classname);
			*/
		}


		//--------------------------------------------------------------------------------------------------------------


		/*
		System.out.println("Building job 5...");
		Job job5 = Job.getInstance(conf);
		job5.setJarByClass(Job5Sort.class);

		job5.setInputFormatClass(SequenceFileInputFormat.class);
		job5.setOutputFormatClass(TextOutputFormat.class);

		job5.setMapperClass(Job5Sort.CastlingMapper.class);
		job5.setMapOutputKeyClass(StringStringDoubleTriple.class);
		job5.setMapOutputValueClass(Text.class);

		job5.setReducerClass(Job5Sort.FinisherReducer.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(DoubleWritable.class);

		job5.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job5, new Path(args[0] + ""));
		FileOutputFormat.setOutputPath(job5, new Path(args[0] + "FinalOutput"));

		System.out.println("Done building!\n" +
		                   "Starting job 5...");
		System.out.println("Job 5 completed with success status: " + job5.waitForCompletion(true) + "!\n" +
		                   "Exiting...");
		*/
	}

	private static SequenceInputStream bucketBatch(S3Client s3Client, String bucketName, String folderPrefix) throws UnsupportedEncodingException
	{
		System.out.println("Deleting bucket Batch...");

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
						"@attribute class {similar, not-similar}\n" +
						"\n" +
						"@data\n") : sequenceInputStream,
						new SequenceInputStream(new Enumeration<ResponseInputStream<GetObjectResponse>>()
						{
							private final Iterator<ResponseInputStream<GetObjectResponse>> iterator = finalListObjectsV2Response.contents().stream()
									.map(S3Object::key)
									.peek(key -> System.out.println("key: " + key))
									.filter(key -> key.startsWith("part-r-"))
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
