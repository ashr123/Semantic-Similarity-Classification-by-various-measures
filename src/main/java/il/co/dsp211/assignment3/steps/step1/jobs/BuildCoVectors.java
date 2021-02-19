package il.co.dsp211.assignment3.steps.step1.jobs;

import il.co.dsp211.assignment3.steps.utils.IntegerLongPair;
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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BuildCoVectors
{
	private static Set<String> readGoldenStandardToSet(Mapper<?, ?, ?, ?>.Context context) throws IOException
	{
		try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream(context.getConfiguration().get("goldenStandardFileName")))))
		{
			return bufferedReader.lines().parallel()
					.map(line -> line.split("\t"))
					.flatMap(strings -> Stream.of(strings[0], strings[1]))
					.collect(Collectors.toSet());
		}
	}

	public static class VectorRecordFilterMapper extends Mapper<LongWritable, Text, Text, StringStringPair>
	{
		private Set<String> goldenStandardWords;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			goldenStandardWords = readGoldenStandardToSet(context);
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
		private Set<String> goldenStandardWords;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			goldenStandardWords = readGoldenStandardToSet(context);
		}

		/**
		 * @param key     ⟨⟨word, dep label⟩,
		 * @param value   count(l)⟩
		 * @param context ⟨word, ⟨"Count_L_Label", count(l) (as string)⟩⟩
		 */
		@Override
		protected void map(StringStringPair key, LongWritable value, Context context) throws IOException, InterruptedException
		{
			if (key.getDepLabel().equals("Count_L_Label") && goldenStandardWords.contains(key.getWord()))
				context.write(new Text(key.getWord()), new StringStringPair("Count_L_Label", value.toString()));
		}
	}

	public static class CalculateEmbeddingsReducer extends Reducer<Text, StringStringPair, Text, VectorsQuadruple>
	{
		private Map<StringStringPair, IntegerLongPair> features;
		private long[] vectorLittleF;
		private long counterFL;

		@Override
		protected void setup(Context context) throws IOException
		{
			try (FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			     BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path("features.txt")))))
			{
				features = bufferedReader.lines().parallel()
						.map(s -> s.split("\t"))
						.collect(Collectors.toMap(strings -> StringStringPair.of(strings[0]), strings -> new IntegerLongPair(Integer.parseInt(strings[1]), Long.parseLong(strings[2]))));
			}

			vectorLittleF = new long[features.size()/*features.values().parallelStream()
					.map(IntegerLongPair::getKey)
					.mapToInt(Integer::valueOf)
					.max()
					.orElse(0)*/];
			features.values().parallelStream()
					.forEach(iAndCountF -> vectorLittleF[iAndCountF.getKey()] = iAndCountF.getValue());

			counterFL = context.getConfiguration().getLong("CounterFL", -1);
		}

		/**
		 * @param key     ⟨word,
		 * @param values  [⟨word, dep label⟩ | ⟨"Count_L_Label", count(l) (as {@link Text})⟩]⟩
		 * @param context ⟨word, ⟨vector5, vector6, vector7, vector8⟩⟩
		 */
		@Override
		protected void reduce(Text key, Iterable<StringStringPair> values, Context context) throws IOException, InterruptedException
		{
			final LongWritable[] vector5 = new LongWritable[vectorLittleF.length];
			final DoubleWritable[]
					vector6 = new DoubleWritable[vectorLittleF.length],
					vector7 = new DoubleWritable[vectorLittleF.length],
					vector8 = new DoubleWritable[vectorLittleF.length];
			IntStream.range(0, vectorLittleF.length).parallel().forEach(i ->
			{
				vector5[i] = new LongWritable();
				vector6[i] = new DoubleWritable();
				vector7[i] = new DoubleWritable();
				vector8[i] = new DoubleWritable();
			});

			long countLittleL = -1;

			// Calc Vector 5
			for (final StringStringPair next : values)
			{
				if (next.getWord().equals("Count_L_Label"))
				{
					countLittleL = Long.parseLong(next.getDepLabel());
				} else if (features.containsKey(next))
				{
					final int i = features.get(next).getKey();
					vector5[i].set(vector5[i].get() + 1);
				}
			}

			if (countLittleL == -1)
			{
				throw new IllegalStateException("Count Little fucking L");
			}

			// Calc Vectors 6-8
			final long finalCountLittleL = countLittleL;
			IntStream.range(0, vectorLittleF.length).parallel().forEach(i ->
			{
				final double
						probLittleL = 1.0 * finalCountLittleL / counterFL,
						probLittleF = 1.0 * vectorLittleF[i] / counterFL;
				vector6[i].set(1.0 * vector5[i].get() / finalCountLittleL);
				vector7[i].set(Math.log10((1.0 * vector5[i].get()) / counterFL / (probLittleL * probLittleF)) / Math.log10(2));
				vector8[i].set((1.0 * vector5[i].get() / counterFL - probLittleL * probLittleF) / Math.sqrt(probLittleL * probLittleF));
			});

			context.write(key, new VectorsQuadruple(vector5, vector6, vector7, vector8));
		}
	}
}
