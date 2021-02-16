package il.co.dsp211.assignment3.steps.step1.jobs;

import il.co.dsp211.assignment3.steps.utils.StringStringPair;
import il.co.dsp211.assignment3.steps.utils.VectorsQuadruple;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class BuildCoVectors
{
	private static Set<String> GOLDEN_STANDARD_WORDS = null;

	static
	{
		//noinspection ConstantConditions
		if (GOLDEN_STANDARD_WORDS == null)
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

	public static class VectorRecordFilterMapper extends Mapper<LongWritable, Text, Text, StringStringPair>
	{
		private static final LongWritable ONE = new LongWritable(1);

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
				if (headIndex != 0 && GOLDEN_STANDARD_WORDS.contains(token[0]))
					context.write(new Text(token[0]), new StringStringPair(tokens[headIndex].split("/")[0], token[2]));
			}
		}
	}

	public static class PairSummerCombinerAndReducer extends Reducer<Text, StringStringPair, Text, VectorsQuadruple>
	{
		private Map<StringStringPair, Short> map;

		@Override
		protected void setup(Context context) throws IOException
		{
			try (FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			     BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path("features")))))
			{
				map = bufferedReader.lines().parallel()
						.map(s -> s.split("\t"))
						.collect(Collectors.toMap(strings -> StringStringPair.of(strings[0]), strings -> Short.valueOf(strings[1])));
			}
		}

		/**
		 * @param key     ⟨⟨word, dep label⟩,
		 * @param values  for combiner: [1], for reducer: [number]⟩
		 * @param context ⟨⟨word, dep label⟩, sum⟩
		 */
		@Override
		protected void reduce(Text key, Iterable<StringStringPair> values, Context context) throws IOException, InterruptedException
		{
			final LongWritable[] vector5 = new LongWritable[1000];
			final DoubleWritable[]
					vector6 = new DoubleWritable[1000],
					vector7 = new DoubleWritable[1000],
					vector8 = new DoubleWritable[1000];
			Arrays.parallelSetAll(vector5, LongWritable::new);
			Arrays.parallelSetAll(vector6, DoubleWritable::new);
			Arrays.parallelSetAll(vector7, DoubleWritable::new);
			Arrays.parallelSetAll(vector8, DoubleWritable::new);

			StreamSupport.stream(values.spliterator(), false)
					.filter(map::containsKey)
					.mapToInt(map::get)
					.forEach(i ->
					{
						vector5[i].set(vector5[i].get() + 1);
						// toto vector6
						// todo vector7
						// toto vector8
					});
			context.write(key, new VectorsQuadruple(vector5, vector6, vector7, vector8));
		}
	}
}
