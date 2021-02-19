package il.co.dsp211.assignment3.steps.step1.jobs;

import il.co.dsp211.assignment3.steps.utils.NCounter;
import il.co.dsp211.assignment3.steps.utils.StringStringPair;
import il.co.dsp211.assignment3.steps.utils.VectorsQuadruple;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class BuildDistancesVectors
{
	public static class BuildMatchingCoVectorsMapper extends Mapper<Text, VectorsQuadruple, StringStringPair, VectorsQuadruple>
	{
		private Map<String, Map<String, Boolean>> goldenStandard;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream(context.getConfiguration().get("goldenStandardFileName")))))
			{
				goldenStandard = bufferedReader.lines()
						.map(line -> line.split("\t"))
						.collect(Collectors.groupingBy(strings -> strings[0],
								Collectors.toMap(strings -> strings[1], strings -> Boolean.valueOf(strings[2]))));
			}
		}

		/**
		 * @param key     ⟨word,
		 * @param value   ⟨vector5, vector6, vector7, vector8⟩⟩
		 * @param context ⟨⟨word<sub>1</sub>, dep label⟩, 1⟩, ⟨⟨word<sub>2</sub>, dep label⟩, 1⟩, ⟨⟨word<sub>3</sub>, dep label⟩, 1⟩
		 */
		@Override
		protected void map(Text key, VectorsQuadruple value, Context context) throws IOException, InterruptedException
		{

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
