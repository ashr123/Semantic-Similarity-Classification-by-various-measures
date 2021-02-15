package il.co.dsp211.assignment3.steps.step1.jobs;

import il.co.dsp211.assignment3.steps.utils.DepLabels;
import il.co.dsp211.assignment3.steps.utils.StringDepLabelPair;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class BuildCoVectors
{
	public static class VectorRecordFilterMapper extends Mapper<LongWritable, Text, Text, StringDepLabelPair>
	{
		private static final LongWritable ONE = new LongWritable(1);
		private static Set<String> GOLDEN_STANDARD_WORDS = null;

		static
		{
			//noinspection ConstantConditions
			if (GOLDEN_STANDARD_WORDS == null)
			{
				synchronized (VectorRecordFilterMapper.class)
				{
					if (GOLDEN_STANDARD_WORDS == null)
						try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(VectorRecordFilterMapper.class.getResourceAsStream("word-relatedness.txt"))))
						{
							GOLDEN_STANDARD_WORDS = bufferedReader.lines().parallel()
									.map(line -> line.split("\t"))
									.flatMap(strings -> Stream.of(strings[0], strings[1]))
									.collect(Collectors.toSet());
						}
						catch (IOException e)
						{
							throw new IllegalStateException(e);
						}
				}
			}
		}

		/**
		 * @param key     ⟨line number,
		 * @param value   ⟨head word, ⟨⟨word<sub>1</sub>, pos tag, dep label, head index<sub>1</sub>⟩, ⟨word<sub>2</sub>, pos tag, dep label, head index<sub>2</sub>⟩, ⟨word<sub>3</sub>, pos tag, dep label, head index<sub>3</sub>⟩⟩, total count, counts by year⟩
		 * @param context ⟨word<sub>1</sub>, ⟨word<sub>head index₁</sub>, dep label<sub>1</sub>⟩⟩, ⟨word<sub>2</sub>, ⟨word<sub>head index₂</sub>, dep label<sub>2</sub>⟩⟩, ⟨word<sub>3</sub>, ⟨word<sub>head index₃</sub>, dep label<sub>3</sub>⟩⟩
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			final String[] split = value.toString().split("\t");
			final String[] tokens = split[1].split(" ");
			for (String tokensSplit : tokens)
			{
				final String[] token = tokensSplit.split("/");
				final int headIndex = Integer.parseInt(token[3]);
				if (GOLDEN_STANDARD_WORDS.contains(token[0]))
					context.write(new Text(token[0]), new StringDepLabelPair(headIndex == 0 ? split[0] : tokens[headIndex].split("/")[0], DepLabels.valueOf(token[2])));
			}
		}
	}

	public static class PairSummerCombinerAndReducer extends Reducer<Text, StringDepLabelPair, Text, ArrayWritable>
	{
		/**
		 * TODO implement
		 */
		Map<StringDepLabelPair, Short> mapStab = new HashMap<>();

		/**
		 * @param key     ⟨⟨word, dep label⟩,
		 * @param values  for combiner: [1], for reducer: [number]⟩
		 * @param context ⟨⟨word, dep label⟩, sum⟩
		 */
		@Override
		protected void reduce(Text key, Iterable<StringDepLabelPair> values, Context context) throws IOException, InterruptedException
		{
			final LongWritable[] writables = new LongWritable[1000];
			Arrays.parallelSetAll(writables, LongWritable::new);

			StreamSupport.stream(values.spliterator(), false)
					.filter(mapStab::containsKey)
					.mapToInt(mapStab::get)
					.forEach(i -> writables[i].set(writables[i].get() + 1));
			context.write(key, new ArrayWritable(LongWritable.class, writables));
		}
	}
}
