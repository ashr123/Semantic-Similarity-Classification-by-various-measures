package il.co.dsp211.assignment3.steps.step1.jobs;

import il.co.dsp211.assignment3.steps.utils.*;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.IntStream;

public class BuildDistancesVectors
{
	public static class BuildMatchingCoVectorsMapper extends Mapper<Text, VectorsQuadruple, StringBooleanPair, StringVectorsQuadruplePair>
	{
		/**
		 * @param key     ⟨word,
		 * @param value   ⟨vector5, vector6, vector7, vector8⟩⟩
		 * @param context ⟨⟨word1, isNotFirst⟩, ⟨word2, VectorsQuadruple⟩⟩
		 */
		@Override
		protected void map(Text key, VectorsQuadruple value, Context context) throws IOException, InterruptedException
		{
			context.write(new StringBooleanPair(key.toString(), false), new StringVectorsQuadruplePair("", value));

			if (GoldenStandard.getGoldenStandard(context.getConfiguration()).containsKey(key.toString()))
			{
				for (String neighbor : GoldenStandard.getGoldenStandard(context.getConfiguration()).get(key.toString()).keySet())
				{
					context.write(new StringBooleanPair(neighbor, true), new StringVectorsQuadruplePair(key.toString(), value));
				}
			}
		}
	}
	public static class CreatePairDistancesVectorReducer extends Reducer<StringBooleanPair, StringVectorsQuadruplePair, ArrayWritable, BooleanWritable>
	{
		private VectorsQuadruple mainWordVectors;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			mainWordVectors = null;
		}

		/**
		 * @param key     ⟨⟨word1, isNotFirst⟩,
		 * @param values  ⟨word2, VectorsQuadruple⟩⟩
		 * @param context ⟨⟨word1, word2⟩, 24-Vector⟩
		 */
		@Override
		protected void reduce(StringBooleanPair key, Iterable<StringVectorsQuadruplePair> values, Context context) throws IOException, InterruptedException
		{
			if (key.isValue())
			{
				if (mainWordVectors != null)
				{
					for (StringVectorsQuadruplePair next : values)
					{
						final LongWritable[]
								vector5_word1 = mainWordVectors.getVector5(),
								vector5_word2 = next.getValue().getVector5();
						final DoubleWritable[]
								vector6_word1 = mainWordVectors.getVector6(),
								vector6_word2 = next.getValue().getVector6(),
								vector7_word1 = mainWordVectors.getVector7(),
								vector7_word2 = next.getValue().getVector7(),
								vector8_word1 = mainWordVectors.getVector8(),
								vector8_word2 = next.getValue().getVector8(),
								vector24D = new DoubleWritable[24];

						final double[]
								// DistManhattan - Init variables
								sumArrayManhattanTemp = new double[4],
								// DistEuclidean - Init variables
								sumArrayEuclideanTemp = new double[4],
								// simCosine - Init variables
								sumMulTemp = new double[4],
								sumV1SquareTemp = new double[4],
								sumV2SquareTemp = new double[4],
								// simJaccard - Init variables
								sumMinTemp = new double[4],
								sumMaxTemp = new double[4],
								//simDice - Init variables
								sumAddTemp = new double[4],
								// simJS - Init variables
								sumD1Temp = new double[4],
								sumD2Temp = new double[4];

						IntStream.range(0, vector5_word1.length).parallel().forEach(i ->
						{
							// Dist - Manhattan - Calc
							sumArrayManhattanTemp[0] += Math.abs(vector5_word1[i].get() - vector5_word2[i].get());
							sumArrayManhattanTemp[1] += Math.abs(vector6_word1[i].get() - vector6_word2[i].get());
							sumArrayManhattanTemp[2] += Math.abs(vector7_word1[i].get() - vector7_word2[i].get());
							sumArrayManhattanTemp[3] += Math.abs(vector8_word1[i].get() - vector8_word2[i].get());

							// Dist - Euclidean - Calc
							sumArrayEuclideanTemp[0] += 1 << Math.abs(vector5_word1[i].get() - vector5_word2[i].get());
							sumArrayEuclideanTemp[1] += Math.pow(vector6_word1[i].get() - vector6_word2[i].get(), 2);
							sumArrayEuclideanTemp[2] += Math.pow(vector7_word1[i].get() - vector7_word2[i].get(), 2);
							sumArrayEuclideanTemp[3] += Math.pow(vector8_word1[i].get() - vector8_word2[i].get(), 2);

							// simCosine
							sumMulTemp[0] += vector5_word1[i].get() * vector5_word2[i].get();
							sumMulTemp[1] += vector6_word1[i].get() * vector6_word2[i].get();
							sumMulTemp[2] += vector7_word1[i].get() * vector7_word2[i].get();
							sumMulTemp[3] += vector8_word1[i].get() * vector8_word2[i].get();

							sumV1SquareTemp[0] += 1 << Math.abs(vector5_word1[i].get());
							sumV1SquareTemp[1] += Math.pow(vector6_word1[i].get(), 2);
							sumV1SquareTemp[2] += Math.pow(vector7_word1[i].get(), 2);
							sumV1SquareTemp[3] += Math.pow(vector8_word1[i].get(), 2);

							sumV2SquareTemp[0] += 1 << Math.abs(vector5_word2[i].get());
							sumV2SquareTemp[1] += Math.pow(vector6_word2[i].get(), 2);
							sumV2SquareTemp[2] += Math.pow(vector7_word2[i].get(), 2);
							sumV2SquareTemp[3] += Math.pow(vector8_word2[i].get(), 2);

							// simJaccard
							sumMinTemp[0] += Math.min(vector5_word1[i].get(), vector5_word2[i].get());
							sumMinTemp[1] += Math.min(vector6_word1[i].get(), vector6_word2[i].get());
							sumMinTemp[2] += Math.min(vector7_word1[i].get(), vector7_word2[i].get());
							sumMinTemp[3] += Math.min(vector8_word1[i].get(), vector8_word2[i].get());

							sumMaxTemp[0] += Math.max(vector5_word1[i].get(), vector5_word2[i].get());
							sumMaxTemp[1] += Math.max(vector6_word1[i].get(), vector6_word2[i].get());
							sumMaxTemp[2] += Math.max(vector7_word1[i].get(), vector7_word2[i].get());
							sumMaxTemp[3] += Math.max(vector8_word1[i].get(), vector8_word2[i].get());

							// simDice
							sumAddTemp[0] += vector5_word1[i].get() + vector5_word2[i].get();
							sumAddTemp[1] += vector6_word1[i].get() + vector6_word2[i].get();
							sumAddTemp[2] += vector7_word1[i].get() + vector7_word2[i].get();
							sumAddTemp[3] += vector8_word1[i].get() + vector8_word2[i].get();

							// simJS
							sumD1Temp[0] += vector5_word1[i].get() * Math.log(vector5_word1[i].get() / (sumAddTemp[0] / 2));
							sumD1Temp[1] += vector6_word1[i].get() * Math.log(vector6_word1[i].get() / (sumAddTemp[1] / 2));
							sumD1Temp[2] += vector7_word1[i].get() * Math.log(vector7_word1[i].get() / (sumAddTemp[2] / 2));
							sumD1Temp[3] += vector8_word1[i].get() * Math.log(vector8_word1[i].get() / (sumAddTemp[3] / 2));

							sumD2Temp[0] += vector5_word2[i].get() * Math.log(vector5_word2[i].get() / (sumAddTemp[0] / 2));
							sumD2Temp[1] += vector6_word2[i].get() * Math.log(vector6_word2[i].get() / (sumAddTemp[1] / 2));
							sumD2Temp[2] += vector7_word2[i].get() * Math.log(vector7_word2[i].get() / (sumAddTemp[2] / 2));
							sumD2Temp[3] += vector8_word2[i].get() * Math.log(vector8_word2[i].get() / (sumAddTemp[3] / 2));
						});

						IntStream.range(0, 4).parallel().forEach(i ->
						{
							// DistManhattan - results
							vector24D[i * 6] = new DoubleWritable(sumArrayManhattanTemp[i]);
							// DistEuclidean - results
							vector24D[i * 6 + 1] = new DoubleWritable(Math.sqrt(sumArrayEuclideanTemp[i]));
							// simCosine - results
							vector24D[i * 6 + 2] = new DoubleWritable(sumMulTemp[i] / Math.sqrt(sumV1SquareTemp[i]) / Math.sqrt(sumV2SquareTemp[i]));
							// simJaccard - results
							vector24D[i * 6 + 3] = new DoubleWritable(sumMinTemp[i] / sumMaxTemp[i]);
							// simDice - results
							vector24D[i * 6 + 4] = new DoubleWritable(2 * sumMinTemp[i] / sumAddTemp[i]);
							// simJS - results
							vector24D[i * 6 + 5] = new DoubleWritable(sumD1Temp[i] + sumD2Temp[i]);
						});

						context.write(new ArrayWritable(DoubleWritable.class, vector24D),
								new BooleanWritable(GoldenStandard.getGoldenStandard(context.getConfiguration())
										.get(next.getKey())
										.get(key.getKey())));
					}
					mainWordVectors = null;
				}
			} else
			{
				boolean seen = false;
				for (StringVectorsQuadruplePair next : values)
				{
					if (seen)
						throw new IllegalStateException("ERROR: Should not have more than 1 record like: ⟨⟨word1, isNotFirst=false⟩, ⟨word2=\"\", VectorsQuadruple⟩⟩");
					seen = true;
					mainWordVectors = next.getValue();
				}
			}
		}
	}

	public static class JoinPartitioner extends Partitioner<StringBooleanPair, StringVectorsQuadruplePair>
	{
		/**
		 * Ensures that record with with same head-word are directed to the same reducer
		 *
		 * @param key           the key to be partitioned.
		 * @param value         the entry value.
		 * @param numPartitions the total number of partitions.
		 * @return the partition number for the <code>key</code>.
		 */
		@Override
		public int getPartition(StringBooleanPair key, StringVectorsQuadruplePair value, int numPartitions)
		{
			return (key.getKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
}
