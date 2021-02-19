package il.co.dsp211.assignment3.steps.step1.jobs;

import il.co.dsp211.assignment3.steps.utils.StringBooleanPair;
import il.co.dsp211.assignment3.steps.utils.StringStringPair;
import il.co.dsp211.assignment3.steps.utils.StringVectorsQuadruplePair;
import il.co.dsp211.assignment3.steps.utils.VectorsQuadruple;
import org.apache.hadoop.io.ArrayWritable;
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
import java.util.stream.Collectors;

public class BuildDistancesVectors {
	public static class BuildMatchingCoVectorsMapper extends Mapper<Text, VectorsQuadruple, StringBooleanPair, StringVectorsQuadruplePair> {
		private Map<String, Map<String, Boolean>> goldenStandard;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream(context.getConfiguration().get("goldenStandardFileName"))))) {
				goldenStandard = bufferedReader.lines()
						.map(line -> line.split("\t"))
						.collect(Collectors.groupingBy(strings -> strings[0],
								Collectors.toMap(strings -> strings[1], strings -> Boolean.valueOf(strings[2]))));
			}
		}

		/**
		 * @param key     ⟨word,
		 * @param value   ⟨vector5, vector6, vector7, vector8⟩⟩
		 * @param context ⟨⟨word1, isNotFirst⟩, ⟨word2, VectorsQuadruple⟩⟩
		 */
		@Override
		protected void map(Text key, VectorsQuadruple value, Context context) throws IOException, InterruptedException {
			context.write(new StringBooleanPair(key.toString(), false), new StringVectorsQuadruplePair("", value));

			for (String neighbor : goldenStandard.get(key.toString()).keySet()) {
				context.write(new StringBooleanPair(neighbor, true), new StringVectorsQuadruplePair(key.toString(), value));
			}
		}
	}

	public static class tempNameReducer extends Reducer<StringBooleanPair, StringVectorsQuadruplePair, StringStringPair, ArrayWritable> {
		VectorsQuadruple mainWordVectors = null;

		/**
		 * @param key     ⟨⟨word1, isNotFirst⟩,
		 * @param values  ⟨word2, VectorsQuadruple⟩⟩
		 * @param context ⟨⟨word1, word2⟩, 24-Vector⟩
		 */
		@Override
		protected void reduce(StringBooleanPair key, Iterable<StringVectorsQuadruplePair> values, Context context) throws IOException, InterruptedException {
			if (key.isValue()) {
				if (mainWordVectors != null) {
					for (StringVectorsQuadruplePair next : values) {
						final DoubleWritable[] vector24D = new DoubleWritable[24];

						LongWritable[] vector5_word1 = mainWordVectors.getVector5();
						LongWritable[] vector5_word2 = next.getValue().getVector5();
						DoubleWritable[] vector6_word1 = mainWordVectors.getVector6();
						DoubleWritable[] vector6_word2 = next.getValue().getVector6();
						DoubleWritable[] vector7_word1 = mainWordVectors.getVector7();
						DoubleWritable[] vector7_word2 = next.getValue().getVector7();
						DoubleWritable[] vector8_word1 = mainWordVectors.getVector8();
						DoubleWritable[] vector8_word2 = next.getValue().getVector8();

						// Dist - Manhattan - Init variables
						double[] sumArrayManhattan = new double[4];

						// Dist - Euclidean - Init variables
						double[] sumArrayEuclidean = new double[4];

						for (int i = 0; i < 1000; i++) {
							// Dist - Manhattan - Calc
							sumArrayManhattan[0] += Math.abs(vector5_word1[i].get() - vector5_word2[i].get());
							sumArrayManhattan[1] += Math.abs(vector6_word1[i].get() - vector6_word2[i].get());
							sumArrayManhattan[2] += Math.abs(vector7_word1[i].get() - vector7_word2[i].get());
							sumArrayManhattan[3] += Math.abs(vector8_word1[i].get() - vector8_word2[i].get());

							// Dist - Euclidean - Calc
							sumArrayEuclidean[0] += 1 << (vector5_word1[i].get() - vector5_word2[i].get());
							sumArrayEuclidean[1] += Math.pow(vector6_word1[i].get() - vector6_word2[i].get(), 2);
							sumArrayEuclidean[2] += Math.pow(vector7_word1[i].get() - vector7_word2[i].get(), 2);
							sumArrayEuclidean[3] += Math.pow(vector8_word1[i].get() - vector8_word2[i].get(), 2);
						}

						// Dist - Manhattan - Assign results
						vector24D[] = new DoubleWritable(sumArrayManhattan[0]);
						vector24D[] = new DoubleWritable(sumArrayManhattan[1]);
						vector24D[] = new DoubleWritable(sumArrayManhattan[2]);
						vector24D[] = new DoubleWritable(sumArrayManhattan[3]);

						// Dist - Euclidean - Assign results
						vector24D[] = new DoubleWritable(Math.sqrt(sumArrayEuclidean[0]));
						vector24D[] = new DoubleWritable(sumArrayEuclidean[1]);
						vector24D[] = new DoubleWritable(sumArrayEuclidean[2]);
						vector24D[] = new DoubleWritable(sumArrayEuclidean[3]);

						}
						context.write(new StringStringPair(key.getKey(), next.getKey()), new ArrayWritable(DoubleWritable.class, vector24D));
					}
				} else {
					throw new IllegalStateException("ERROR: mainWordVectors should be initialized!");
				}
			} else {
				int count = 0;
				for (StringVectorsQuadruplePair next : values) {
					if (count > 0) {
						throw new IllegalStateException("ERROR: Should not have more than 1 record like: ⟨⟨word1, isNotFirst=false⟩, ⟨word2=\"\", VectorsQuadruple⟩⟩");
					}
					count++;
					mainWordVectors = next.getValue();
				}
			}
		}
	}
}
