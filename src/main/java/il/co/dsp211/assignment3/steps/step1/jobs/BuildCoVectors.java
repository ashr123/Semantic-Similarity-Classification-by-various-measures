package il.co.dsp211.assignment3.steps.step1.jobs;

import il.co.dsp211.assignment3.steps.utils.ShortLongPair;
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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BuildCoVectors
{
	public static class VectorRecordFilterMapper extends Mapper<LongWritable, Text, Text, StringStringPair>
	{
		private Set<String> goldenStandardWords;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream("word-relatedness.txt"))))
			{
				goldenStandardWords = bufferedReader.lines().parallel()
						.map(line -> line.split("\t"))
						.flatMap(strings -> Stream.of(strings[0], strings[1]))
						.collect(Collectors.toSet());
			}
		}

		/**
		 * @param key     ⟨line number,
		 * @param value   ⟨head word, ⟨⟨word<sub>1</sub>, pos tag, dep label, head index⟩, ⟨word<sub>2</sub>, pos tag, dep label, head index⟩, ⟨word<sub>3</sub>, pos tag, dep label, head index⟩⟩, total count, counts by year⟩
		 * @param context ⟨word, ⟨word, dep label⟩⟩
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			final String[] tokens = value.toString().split("\t")[1].split(" ");
			for (final String tokensSplit : tokens)
			{
				final String[] token = tokensSplit.split("/");
				if (token.length != 4)
					continue;
				final int headIndex = Integer.parseInt(token[3]);
				if (headIndex != 0 && headIndex < tokens.length && goldenStandardWords.contains(token[0]))
					context.write(new Text(token[0]), new StringStringPair(tokens[headIndex].split("/")[0], token[2]));
			}
		}
	}

	public static class CounterLittleLMapper extends Mapper<StringStringPair, LongWritable, Text, StringStringPair>
	{
		/**
		 * @param key     ⟨⟨word, dep label⟩,
		 * @param value   count(f)⟩
		 * @param context ⟨word, ⟨"", count(f) (as string)⟩⟩ ????????
		 */
		@Override
		protected void map(StringStringPair key, LongWritable value, Context context) throws IOException, InterruptedException
		{
			if (key.getDepLabel().isEmpty())
				context.write(new Text(key.getWord()), new StringStringPair("", value.toString()));
		}
	}

	public static class CalculateEmbeddingsReducer extends Reducer<Text, StringStringPair, Text, VectorsQuadruple>
	{
		private Map<StringStringPair, ShortLongPair> map;
		private long counterFL;

		@Override
		protected void setup(Context context) throws IOException
		{
			try (FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			     BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path("features.txt")))))
			{
				map = bufferedReader.lines().parallel()
						.map(s -> s.split("\t"))
						.collect(Collectors.toMap(strings -> StringStringPair.of(strings[0]), strings -> new ShortLongPair(Short.parseShort(strings[1]), Long.parseLong(strings[2]))));
			}

			counterFL = context.getConfiguration().getLong("CounterFL", -1);
		}

		/**
		 * @param key     ⟨word,
		 * @param values  [⟨word, dep label⟩ | ⟨"", count(f) (as string)⟩]⟩
		 * @param context
		 */
		@Override
		protected void reduce(Text key, Iterable<StringStringPair> values, Context context) throws IOException, InterruptedException
		{
			final LongWritable[] vector5 = new LongWritable[1000];
			final DoubleWritable[]
					vector6 = new DoubleWritable[1000],
					vector7 = new DoubleWritable[1000],
					vector8 = new DoubleWritable[1000];
			Arrays.parallelSetAll(vector5, value -> new LongWritable());
			final IntFunction<DoubleWritable> doubleWritableIntFunction = value -> new DoubleWritable();
			Arrays.parallelSetAll(vector6, doubleWritableIntFunction);
			Arrays.parallelSetAll(vector7, doubleWritableIntFunction);
			Arrays.parallelSetAll(vector8, doubleWritableIntFunction);

			final Iterator<StringStringPair> iterator = values.iterator();

			long countLittleL = -1;

			// Calc Vector 5
			for (final StringStringPair next : values)
				if (next.getWord().isEmpty())
					countLittleL = Long.parseLong(iterator.next().getDepLabel());
				else if (map.containsKey(next))
				{
					final short i = map.get(next).getKey();
					vector5[i].set(vector5[i].get() + 1);
				}

			// Calc Vectors 6-8
			for (final StringStringPair next : values)
				if (map.containsKey(next))
				{
					final short i = map.get(next).getKey();
					final double
							probLittleL = 1.0 * countLittleL / counterFL,
							probLittleF = 1.0 * map.get(next).getValue() / counterFL;
					vector6[i].set(1.0 * vector5[i].get() / countLittleL);
					vector7[i].set(Math.log10((1.0 * vector5[i].get()) / counterFL / (probLittleL * probLittleF)) / Math.log10(2));
					vector8[i].set((1.0 * vector5[i].get() / counterFL - probLittleL * probLittleF) / Math.sqrt(probLittleL * probLittleF));
				}
			context.write(key, new VectorsQuadruple(vector5, vector6, vector7, vector8));
//			context.write(key, new ArrayWritable(LongWritable.class, vector5));
		}
	}
}
