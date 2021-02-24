package il.co.dsp211.assignment3.steps.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StringBooleanPair implements WritableComparable<StringBooleanPair>
{
	private String key;
	private boolean value;

	public StringBooleanPair()
	{
	}

	public StringBooleanPair(String key, boolean value)
	{
		this.key = key;
		this.value = value;
	}

	public static StringBooleanPair of(String string)
	{
		final String[] values = string.split("🤠");
		return new StringBooleanPair(values[0], Boolean.parseBoolean(values[1]));
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		Text.writeString(out, key);
		out.writeBoolean(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		key = Text.readString(in);
		value = in.readBoolean();
	}

	public boolean isValue()
	{
		return value;
	}

	public String getKey()
	{
		return key;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (!(o instanceof StringBooleanPair))
			return false;
		StringBooleanPair that = (StringBooleanPair) o;
		return value == that.value &&
		       key.equals(that.key);
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
	public int compareTo(StringBooleanPair o)
	{
		final int valueCompare = key.compareTo(o.key);
		return valueCompare != 0 ? valueCompare : Boolean.compare(value, o.value);
	}

}
