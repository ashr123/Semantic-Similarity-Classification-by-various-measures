package il.co.dsp211.assignment3.steps.step1.jobs;

import il.co.dsp211.assignment3.steps.utils.DepLabels;
import il.co.dsp211.assignment3.steps.utils.StringDepLabelPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

public class CorpusWordCount
{
	public static class WordCounterMapper extends Mapper<LongWritable, Text, StringDepLabelPair, LongWritable>
	{
		public static final LongWritable ONE = new LongWritable(1);

		/**
		 * @param key     ⟨line number,
		 * @param value   ⟨head word, ⟨⟨word<sub>1</sub>, pos tag, dep label, head index⟩, ⟨word<sub>2</sub>, pos tag, dep label, head index⟩, ⟨word<sub>3</sub>, pos tag, dep label, head index⟩⟩, total count, counts by year⟩
		 * @param context ⟨⟨word<sub>1</sub>, dep label⟩, 1⟩, ⟨⟨word<sub>2</sub>, dep label⟩, 1⟩, ⟨⟨word<sub>3</sub>, dep label⟩, 1⟩
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			final String[] tokens = value.toString().split("\t")[1].split(" ");
			for (String tokensSplit : tokens)
			{
				final String[] token = tokensSplit.split("/");
				context.write(new StringDepLabelPair(token[0], DepLabels.valueOf(token[2])), ONE);
			}
		}
	}

	public static class PairSummerCombinerAndReducer extends Reducer<StringDepLabelPair, LongWritable, StringDepLabelPair, LongWritable>
	{

		/**
		 * @param key     ⟨⟨word, dep label⟩,
		 * @param values  for combiner: [1], for reducer: [number]⟩
		 * @param context ⟨⟨word, dep label⟩, sum⟩
		 */
		@Override
		protected void reduce(StringDepLabelPair key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			context.write(key,
					new LongWritable(StreamSupport.stream(values.spliterator(), false)
							.mapToLong(LongWritable::get)
							.sum()));
		}
	}
}
