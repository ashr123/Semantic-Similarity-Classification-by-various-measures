package il.co.dsp211.assignment3.steps.utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class LongLongPair implements WritableComparable<LongLongPair>
{
	private long key, value;

	public LongLongPair()
	{
	}

	public LongLongPair(long key, long value)
	{
		this.key = key;
		this.value = value;
	}

	public static LongLongPair of(String string)
	{
		final String[] values = string.split("🤠");
		return new LongLongPair(Long.parseLong(values[0]), Long.parseLong(values[1]));
	}

	public long getKey()
	{
		return key;
	}

	public long getValue()
	{
		return value;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (!(o instanceof LongLongPair))
			return false;
		LongLongPair that = (LongLongPair) o;
		return key == that.key &&
		       value == that.value;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(key, value);
	}

	@Override
	public String toString()
	{
		return key + "🤠" + value;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeLong(key);
		out.writeLong(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		key = in.readLong();
		value = in.readLong();
	}

	@Override
	public int compareTo(LongLongPair o)
	{
		final int valueCompare = Long.compare(value, o.value);
		return valueCompare != 0 ? valueCompare : Long.compare(key, o.key);
	}
}
