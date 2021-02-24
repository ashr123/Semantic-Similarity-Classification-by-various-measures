package il.co.dsp211.assignment3.steps.step1.jobs;

import il.co.dsp211.assignment3.steps.utils.NCounter;
import il.co.dsp211.assignment3.steps.utils.StringStringPair;
import opennlp.tools.stemmer.PorterStemmer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class CorpusWordCount
{
	public static class WordCounterMapper extends Mapper<LongWritable, Text, StringStringPair, LongWritable>
	{
		private static final LongWritable ONE = new LongWritable(1);
		private final PorterStemmer porterStemmer = new PorterStemmer();

		/**
		 * @param key     ⟨line number,
		 * @param value   ⟨head word, ⟨⟨word<sub>1</sub>, pos tag, dep label, head index⟩, ⟨word<sub>2</sub>, pos tag, dep label, head index⟩, ⟨word<sub>3</sub>, pos tag, dep label, head index⟩⟩, total count, counts by year⟩⟩
		 * @param context ⟨⟨word<sub>1</sub>, dep label⟩, 1⟩, ⟨⟨word<sub>2</sub>, dep label⟩, 1⟩, ⟨⟨word<sub>3</sub>, dep label⟩, 1⟩
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			final String[] tokens = value.toString().split("\t")[1].split(" ");
			for (String tokensSplit : tokens)
			{
				final String[] token = tokensSplit.split("/");
				if (token.length != 4)
					continue;
				String stemmedWord = porterStemmer.stem(token[0]);
				context.write(new StringStringPair(stemmedWord, token[2]), ONE); // count(f)
				context.write(new StringStringPair(stemmedWord, "Count_L_Label"), ONE); // count(l)
			}
		}
	}

	public static class PairSummerCombinerAndReducer extends Reducer<StringStringPair, LongWritable, StringStringPair, LongWritable>
	{
		private Counter counter;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			counter = context.getCounter(NCounter.N_COUNTER);
		}

		/**
		 * @param key     ⟨⟨word, dep label⟩,
		 * @param values  for combiner: [1], for reducer: [number]⟩
		 * @param context ⟨⟨word, dep label⟩, sum⟩
		 */
		@Override
		protected void reduce(StringStringPair key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			final long sum = StreamSupport.stream(values.spliterator(), false)
					.mapToLong(LongWritable::get)
					.sum();
			context.write(key, new LongWritable(sum));
			counter.increment(sum);
		}
	}
}
