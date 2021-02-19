package il.co.dsp211.assignment3.steps.utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StringStringPairBooleanPair implements WritableComparable<StringStringPairBooleanPair>
{
	private StringStringPair stringStringPair;
	private boolean isNotFirst;

	public StringStringPairBooleanPair()
	{
	}

	public StringStringPairBooleanPair(StringStringPair stringStringPair, boolean isNotFirst)
	{
		this.stringStringPair = stringStringPair;
		this.isNotFirst = isNotFirst;
	}

	public static StringStringPairBooleanPair of(String string)
	{
		final String[] values = string.split("🤠🤠");
		return new StringStringPairBooleanPair(StringStringPair.of(values[0]), Boolean.parseBoolean(values[1]));
	}

	public StringStringPair getStringStringPair()
	{
		return stringStringPair;
	}

	public boolean isNotFirst()
	{
		return isNotFirst;
	}

	@Override
	public int compareTo(StringStringPairBooleanPair o)
	{
		final int stringStringPairCompare = stringStringPair.compareTo(o.stringStringPair);
		return stringStringPairCompare != 0 ? stringStringPairCompare : Boolean.compare(isNotFirst, o.isNotFirst);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		stringStringPair.write(out);
		out.writeBoolean(isNotFirst);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		stringStringPair = StringStringPair.read(in);
		isNotFirst = in.readBoolean();
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (!(o instanceof StringStringPairBooleanPair))
			return false;
		StringStringPairBooleanPair that = (StringStringPairBooleanPair) o;
		return isNotFirst == that.isNotFirst &&
		       stringStringPair.equals(that.stringStringPair);
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(stringStringPair, isNotFirst);
	}

	@Override
	public String toString()
	{
		return stringStringPair + "🤠🤠" + isNotFirst;
	}
}
