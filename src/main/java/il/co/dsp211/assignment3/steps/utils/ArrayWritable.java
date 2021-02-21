package il.co.dsp211.assignment3.steps.utils;

import org.apache.hadoop.io.Writable;

public class ArrayWritable extends org.apache.hadoop.io.ArrayWritable
{

	public ArrayWritable(Class<? extends Writable> valueClass)
	{
		super(valueClass);
	}

	public ArrayWritable(Class<? extends Writable> valueClass, Writable[] values)
	{
		super(valueClass, values);
	}

	public ArrayWritable(String[] strings)
	{
		super(strings);
	}

	static void arraysString(StringBuilder b, Object[] a)
	{
		if (a == null)
		{
			b.append("null");
			return;
		}

		int iMax = a.length - 1;
		if (iMax == -1)
			return;

		for (int i = 0; ; i++)
		{
			b.append(a[i]);
			if (i == iMax)
				return;
			b.append("#");
		}
	}

	@Override
	public String toString()
	{
		final StringBuilder stringBuilder = new StringBuilder();
		arraysString(stringBuilder, super.get());
		return stringBuilder.toString();
	}
}
