package il.co.dsp211.assignment3.steps.step1;

import il.co.dsp211.assignment3.steps.step1.jobs.CorpusPairFilter;
import il.co.dsp211.assignment3.steps.step1.jobs.CorpusWordCount;
import il.co.dsp211.assignment3.steps.utils.StringDepLabelPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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

		System.out.println("Building job 1 - Corpus Word Count...");

		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(CorpusWordCount.class);

		job1.setInputFormatClass(SequenceFileInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		job1.setMapperClass(CorpusWordCount.WordCounterMapper.class);
		job1.setMapOutputKeyClass(StringDepLabelPair.class);
		job1.setMapOutputValueClass(LongWritable.class);

		job1.setCombinerClass(CorpusWordCount.PairSummerCombinerAndReducer.class);

		job1.setReducerClass(CorpusWordCount.PairSummerCombinerAndReducer.class);
		job1.setOutputKeyClass(StringDepLabelPair.class);
		job1.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job1, new Path("s3://Google_Syntactic_N-Grams_URL"));
		FileOutputFormat.setOutputPath(job1, new Path(args[0] + "Step1Output-CorpusWordCount"));

		System.out.println("Done building!\n" +
		                   "Starting job 1 - Corpus Word Count...");
		System.out.println("Job 1 - Corpus Word Count: completed with success status: " + (jobStatus = job1.waitForCompletion(true)) + "!");

		if (!jobStatus)
			return;

		//--------------------------------------------------------------------------------------------------------------

		System.out.println("Building job 2 - CorpusPairFilter...");
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(CorpusPairFilter.class);

		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);

		job2.setMapperClass(CorpusPairFilter.CastlerMapper.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(StringDepLabelPair.class);

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

		System.out.println("Pairs:\n" + conf.get("pairs"));
		//--------------------------------------------------------------------------------------------------------------


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
