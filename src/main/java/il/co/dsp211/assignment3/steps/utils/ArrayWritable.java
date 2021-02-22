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
			b.append(a[i]); // TODO check if '?' is needed or 0
			if (i == iMax)
				return;
			b.append(",");
		}
	}

	static <T extends Comparable<? super T>> int compare(T[] a, T[] b)
	{
		if (a == b)
			return 0;
		// A null array is less than a non-null array
		if (a == null || b == null)
			return a == null ? -1 : 1;

		int length = Math.min(a.length, b.length);
		for (int i = 0; i < length; i++)
		{
			T oa = a[i];
			T ob = b[i];
			if (oa != ob)
			{
				// A null element is less than a non-null element
				if (oa == null || ob == null)
					return oa == null ? -1 : 1;
				int v = oa.compareTo(ob);
				if (v != 0)
					return v;
			}
		}

		return a.length - b.length;
	}

	@Override
	public String toString()
	{
		final StringBuilder stringBuilder = new StringBuilder();
		arraysString(stringBuilder, get());
		return stringBuilder.toString();
	}
}
