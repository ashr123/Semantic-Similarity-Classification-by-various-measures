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
			if (!key.getDepLabel().isEmpty())
				context.write(value, key);
		}
	}

	public static class FilterTopPairsReducer extends Reducer<LongWritable, StringStringPair, Void, Void>
	{
		private final StringBuilder stringBuilder = new StringBuilder();
		private short counter = 0;

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

		/**
		 * <p>IMPORTANT1: need to execute with only 1 reducer in order to maintain valid counter</p>
		 * <p>IMPORTANT2: Use TextOutputFormat!!!</p>
		 *
		 * @param key     ⟨number,
		 * @param values  ⟨word, dep label⟩⟩
		 * @param context ⟨⟨word, dep label⟩, vector index⟩
		 */
		@Override
		protected void reduce(LongWritable key, Iterable<StringStringPair> values, Context context)
		{
			final Iterator<StringStringPair> iterator = values.iterator();
			for (; counter < 100 && iterator.hasNext(); counter++, iterator.next()) ;
			for (; counter < 1100 && iterator.hasNext(); counter++)
			{
				// For next step, we'll find the vector index by ⟨word, dep label⟩ (value = vector index)
				stringBuilder.append(iterator.next()).append('\t').append(counter - 100).append('\n');
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException
		{
			try (FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			     FSDataOutputStream out = fileSystem.create(new Path("features")))
			{
				out.writeUTF(stringBuilder.toString());
			}
		}

//		private void temp(Context context) throws IOException
//		{
//			try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(BuildCoVectors.VectorRecordFilterMapper.class.getResourceAsStream("word-relatedness.txt"))))
//			{
//				GOLDEN_STANDARD_WORDS = bufferedReader.lines().parallel()
//						.map(line -> line.split("\t"))
//						.flatMap(strings -> Stream.of(strings[0], strings[1]))
//						.collect(Collectors.toSet());
//			}


//			FileSystem fileSystem = FileSystem.get(context.getConfiguration());
//// Check if the file already exists
//			//			if (fileSystem.exists(path)) {
////				System.out.println("File " + dest + " already exists");
////				return;
////			}
//// Create a new file and write data to it.
//			FSDataOutputStream out = fileSystem.create(new Path("featurs.ext"));
//
//			InputStream in = new BufferedInputStream(new FileInputStream(new File(source)));
//			byte[] b = new byte[1024];
//			int numBytes = 0;
//			while ((numBytes = in.read(b)) > 0) {
//				out.write(b, 0, numBytes);
//			}
//// Close all the file descripters
//			in.close();
//			out.close();
//			fileSystem.close();
//		}
	}
}
