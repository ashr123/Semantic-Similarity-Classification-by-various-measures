package il.co.dsp211.assignment3.steps.step1.jobs;

import il.co.dsp211.assignment3.steps.utils.StringDepLabelPair;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CorpusPairFilter
{
	public static class CastlerMapper extends Mapper<StringDepLabelPair, LongWritable, LongWritable, StringDepLabelPair>
	{
		/**
		 * @param key     ⟨⟨word, dep label⟩,
		 * @param value   sum⟩
		 * @param context ⟨sum, ⟨word, dep label⟩⟩
		 */
		@Override
		protected void map(StringDepLabelPair key, LongWritable value, Context context) throws IOException, InterruptedException
		{
			context.write(value, key);
		}
	}

	public static class FilterTopPairsReducer extends Reducer<LongWritable, StringDepLabelPair, Void, Void>
	{
		private short counter = 0;
		private final StringBuilder stringBuilder = new StringBuilder();
//		MapWritable mapWritable = new MapWritable();

		@Override
		public void run(Context context) throws IOException, InterruptedException
		{
			setup(context);
			try
			{
				while (counter < 1100 && context.nextKey())
				{
					reduce(context.getCurrentKey(), context.getValues(), context);
					// If a back up store is used, reset it
					Iterator<StringDepLabelPair> iter = context.getValues().iterator();
					if (iter instanceof ReduceContext.ValueIterator)
					{
						((ReduceContext.ValueIterator<StringDepLabelPair>) iter).resetBackupStore();
					}
				}
			}
			finally
			{
				cleanup(context);
			}
		}

		/**
		 * <p>IMPORTANT1: need to execute with only 1 reducer in order to maintain valid counter</p>
		 * <p>IMPORTANT2: Use TextOutputFormat!!!</p>
		 *
		 * @param key     ⟨number,
		 * @param values  ⟨word, dep label⟩⟩
		 * @param context ⟨⟨word, dep label⟩, vector index⟩
		 */
		@Override
		protected void reduce(LongWritable key, Iterable<StringDepLabelPair> values, Context context)
		{
			final Iterator<StringDepLabelPair> iterator = values.iterator();
			for (; counter < 100 && iterator.hasNext(); counter++, iterator.next()) ;
			for (; counter < 1100 && iterator.hasNext(); counter++)
			{
				// For next step, we'll find the vector index by ⟨word, dep label⟩ (value = vector index)
				stringBuilder.append(iterator.next()).append('\t').append(counter - 100).append('\n');
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			context.getConfiguration().set("pairs", stringBuilder.toString());
		}
	}
}
