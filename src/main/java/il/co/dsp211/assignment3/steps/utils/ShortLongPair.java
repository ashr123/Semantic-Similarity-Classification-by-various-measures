package il.co.dsp211.assignment3.steps.utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ShortLongPair implements WritableComparable<ShortLongPair>
{
	private short key;
	private long value;

	public ShortLongPair()
	{
	}

	public ShortLongPair(short key, long value)
	{
		this.key = key;
		this.value = value;
	}

	public static ShortLongPair of(String string)
	{
		final String[] values = string.split("🤠");
		return new ShortLongPair(Short.parseShort(values[0]), Long.parseLong(values[1]));
	}

	public short getKey()
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
		if (!(o instanceof ShortLongPair))
			return false;
		ShortLongPair that = (ShortLongPair) o;
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
		out.writeShort(key);
		out.writeLong(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		key = in.readShort();
		value = in.readLong();
	}

	@Override
	public int compareTo(ShortLongPair o)
	{
		final int valueCompare = Long.compare(value, o.value);
		return valueCompare != 0 ? valueCompare : Short.compare(key, o.key);
	}
}
