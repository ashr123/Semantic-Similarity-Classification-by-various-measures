package il.co.dsp211.assignment3.steps.step1;

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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import weka.classifiers.Classifier;
import weka.core.Debug;
import weka.core.Instances;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class EMR
{
	public static void main(String... args) throws Exception
	{
		boolean jobStatus;
		final Configuration conf = new Configuration();


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

		final Path corpusPath = new Path("s3://assignment3dsp/biarcs" + (Boolean.parseBoolean(args[1]) ? "/biarcs.00-of-99" : ""));
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

		job2.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job2, new Path(args[0] + "Step1Output-CorpusWordCount"));
		FileOutputFormat.setOutputPath(job2, new Path(args[0] + "Step2Output-CorpusPairFilter"));

		System.out.println("Done building!\n" +
		                   "Starting job 2 - CorpusPairFilter...");
		System.out.println("Job 2 - CorpusPairFilter: completed with success status: " + (jobStatus = job2.waitForCompletion(true)) + "!");
		if (!jobStatus)
			return;

//		try (FileSystem fileSystem = FileSystem.get(conf);
//		     BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path("features.txt")))))
//		{
//			System.out.println("Features:");
//			bufferedReader.lines()
//					.map(s -> s.split("\t")[0])
//					.forEach(System.out::println);
//		}

		//--------------------------------------------------------------------------------------------------------------

		System.out.println("Building job 3 - BuildCoVectors...");
		conf.set("goldenStandardFileName", args[2]);

		Job job3 = Job.getInstance(conf);
		job3.setJarByClass(BuildCoVectors.class);

		job3.setInputFormatClass(SequenceFileInputFormat.class);
		job3.setOutputFormatClass(SequenceFileOutputFormat.class);
//		job3.setNumReduceTasks(1);

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
//		jobStatus = false;
		if (!jobStatus)
			return;

		//--------------------------------------------------------------------------------------------------------------

		System.out.println("Building job 4 - BuildDistancesVectors...");

		Job job4 = Job.getInstance(conf);
		job4.setJarByClass(BuildDistancesVectors.class);

		job4.setInputFormatClass(SequenceFileInputFormat.class);

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

		System.out.println("Starting WEKA...");

		// Generate Model
		final ModelGenerator mg = new ModelGenerator();

		String bucketName = args[0].substring(5, args[0].length() - 1);
		try (S3Client s3Client = S3Client.builder()
				.region(Region.of(args[5]))
				.build();
		     InputStream arff = HelperMethods.bucketBatch(s3Client, bucketName, "Step4Output-BuildDistancesVectors/"))
		{
			Instances dataset = mg.loadDataset(arff);

			// divide dataset to train dataset 80% and test dataset 20%
			int trainSize = (int) Math.round(dataset.numInstances() * 0.8);
			int testSize = dataset.numInstances() - trainSize;

			dataset.randomize(new Debug.Random());// if you comment this line the accuracy of the model will be dropped from 96.6% to 80%

			Instances traindataset = new Instances(dataset, 0, trainSize);
			Instances testdataset = new Instances(dataset, trainSize, testSize);

			// build classifier with train dataset
			Classifier ann = mg.buildClassifier(traindataset);

			// Evaluate classifier with test dataset
			String evalsummary = mg.evaluateModel(ann, traindataset, testdataset);
			System.out.println("Evaluation:\n" + evalsummary);

			//Save model
			s3Client.putObject(PutObjectRequest.builder()
					.bucket(bucketName)
					.key("model.bin")
					.build(), RequestBody.fromBytes(HelperMethods.objectToBytes(ann)));
		}

		System.out.println("WEKA completed successfully\n" +
		                   "Exiting...");
	}
}
