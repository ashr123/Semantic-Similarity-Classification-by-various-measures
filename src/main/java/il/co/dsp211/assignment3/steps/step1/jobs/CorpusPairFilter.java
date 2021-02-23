package il.co.dsp211.assignment3.steps.step1.jobs;

import il.co.dsp211.assignment3.steps.utils.StringStringPair;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CorpusPairFilter
{
	public static class CastlerMapper extends Mapper<StringStringPair, LongWritable, LongWritable, StringStringPair>
	{
		/**
		 * @param key     ⟨⟨word, dep label⟩,
		 * @param value   sum⟩
		 * @param context ⟨sum, ⟨word, dep label⟩⟩
		 */
		@Override
		protected void map(StringStringPair key, LongWritable value, Context context) throws IOException, InterruptedException
		{
			if (!key.getDepLabel().equals("Count_L_Label"))
				context.write(value, key);
		}
	}

	public static class FilterTopPairsReducer extends Reducer<LongWritable, StringStringPair, Void, Void>
	{
		private final StringBuilder stringBuilder = new StringBuilder();
		private int counter = 0, numOfFeaturesToSkip, numOfFeatures;

		@Override
		public void run(Context context) throws IOException, InterruptedException
		{
			setup(context);
			try
			{
				while (counter < numOfFeaturesToSkip + numOfFeatures && context.nextKey())
				{
					reduce(context.getCurrentKey(), context.getValues(), context);
					// If a back up store is used, reset it
					final Iterator<StringStringPair> iter = context.getValues().iterator();
					if (iter instanceof ReduceContext.ValueIterator)
					{
						((ReduceContext.ValueIterator<StringStringPair>) iter).resetBackupStore();
					}
				}
			}
			finally
			{
				cleanup(context);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			numOfFeaturesToSkip = context.getConfiguration().getInt("numOfFeaturesToSkip", 0);
			numOfFeatures = context.getConfiguration().getInt("numOfFeatures", 0);
		}

		/**
		 * <p>IMPORTANT1: need to execute with only 1 reducer in order to maintain valid counter</p>
		 * <p>IMPORTANT2: Use TextOutputFormat!!!</p>
		 *
		 * @param key     ⟨count(f),
		 * @param values  [⟨word, dep label⟩]⟩
		 * @param context ⟨⟨word, dep label⟩, vector index, count(f)⟩
		 */
		@Override
		protected void reduce(LongWritable key, Iterable<StringStringPair> values, Context context)
		{
			final Iterator<StringStringPair> iterator = values.iterator();
			for (; counter < numOfFeaturesToSkip && iterator.hasNext(); counter++, iterator.next());
			for (; counter < numOfFeaturesToSkip + numOfFeatures && iterator.hasNext(); counter++)
				stringBuilder.append(iterator.next()).append('\t').append(counter - numOfFeaturesToSkip).append('\t').append(key.get()).append('\n');
		}

		@Override
		protected void cleanup(Context context) throws IOException
		{
			try (FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			     FSDataOutputStream out = fileSystem.create(new Path("features.txt")))
			{
				out.writeUTF(stringBuilder.toString());
			}
		}
	}
}
