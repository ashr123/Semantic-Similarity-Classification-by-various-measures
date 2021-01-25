package il.co.dsp211.assignment3.steps.utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class BooleanLongPair implements WritableComparable<BooleanLongPair>
{
	private boolean key;
	private long value;

	public BooleanLongPair()
	{
	}

	public BooleanLongPair(boolean key, long value)
	{
		this.key = key;
		this.value = value;
	}

	public static BooleanLongPair of(String string)
	{
		final String[] values = string.split("🤠");
		return new BooleanLongPair(Boolean.parseBoolean(values[0]), Long.parseLong(values[1]));
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeBoolean(key);
		out.writeLong(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		key = in.readBoolean();
		value = in.readLong();
	}

	public boolean isKey()
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
		if (!(o instanceof BooleanLongPair))
			return false;
		BooleanLongPair that = (BooleanLongPair) o;
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
	public int compareTo(BooleanLongPair o)
	{
		final int valueCompare = Long.compare(value, o.value);
		return valueCompare != 0 ? valueCompare : Boolean.compare(key, o.key);
	}
}
