package il.co.dsp211.assignment3.steps.step1;

import il.co.dsp211.assignment3.steps.step1.jobs.BuildCoVectors;
import il.co.dsp211.assignment3.steps.step1.jobs.CorpusPairFilter;
import il.co.dsp211.assignment3.steps.step1.jobs.CorpusWordCount;
import il.co.dsp211.assignment3.steps.utils.NCounter;
import il.co.dsp211.assignment3.steps.utils.StringStringPair;
import il.co.dsp211.assignment3.steps.utils.VectorsQuadruple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class EMR
{
	public static void main(String... args) throws IOException, ClassNotFoundException, InterruptedException
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

		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(CorpusPairFilter.class);

		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);

		job2.setMapperClass(CorpusPairFilter.CastlerMapper.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(StringStringPair.class);

		job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		job2.setReducerClass(CorpusPairFilter.FilterTopPairsReducer.class);
//		job2.setOutputKeyClass(StringDepLabelPair.class);
//		job2.setOutputValueClass(LongWritable.class);

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

//		FileInputFormat.addInputPath(job3, corpusPath); // TODO delete
//		job3.setInputFormatClass(SequenceFileInputFormat.class); // TODO delete
//		job3.setMapperClass(BuildCoVectors.VectorRecordFilterMapper.class); // TODO delete

		MultipleInputs.addInputPath(job3, corpusPath, SequenceFileInputFormat.class, BuildCoVectors.VectorRecordFilterMapper.class);
		MultipleInputs.addInputPath(job3, new Path(args[0] + "Step1Output-CorpusWordCount"), SequenceFileInputFormat.class, BuildCoVectors.CounterLittleLMapper.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(StringStringPair.class); // TODO change
//		job3.setOutputFormatClass(SequenceFileOutputFormat.class); TODO uncomment

		job3.setReducerClass(BuildCoVectors.CalculateEmbeddingsReducer.class);
//		job3.setNumReduceTasks(0); // TODO delete
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(VectorsQuadruple.class);


		FileOutputFormat.setOutputPath(job3, new Path(args[0] + "Step3Output-BuildCoVectors"));

		System.out.println("Done building!\n" +
		                   "Starting job 3 - BuildCoVectors...");
		System.out.println("Job 3 - BuildCoVectors: completed with success status: " + (jobStatus = job3.waitForCompletion(true)) + "!");
		if (!jobStatus)
			return;

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
}
