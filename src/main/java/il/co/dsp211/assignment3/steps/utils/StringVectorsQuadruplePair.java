package il.co.dsp211.assignment3.steps.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StringVectorsQuadruplePair implements WritableComparable<StringVectorsQuadruplePair>
{
	private String key;
	private VectorsQuadruple value;

	public StringVectorsQuadruplePair()
	{
	}

	public StringVectorsQuadruplePair(String key, VectorsQuadruple value)
	{
		this.key = key;
		this.value = value;
	}

	public static StringVectorsQuadruplePair of(String string)
	{
		final String[] values = string.split("🤠🤠");
		return new StringVectorsQuadruplePair(values[0], VectorsQuadruple.of(values[1]));
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		Text.writeString(out, key);
		value.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		key = Text.readString(in);
		value = VectorsQuadruple.read(in);
	}

	public VectorsQuadruple getValue()
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
		if (!(o instanceof StringVectorsQuadruplePair))
			return false;
		StringVectorsQuadruplePair that = (StringVectorsQuadruplePair) o;
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
		return key + "🤠🤠" + value;
	}

	@Override
	public int compareTo(StringVectorsQuadruplePair o)
	{
		final int valueCompare = key.compareTo(o.key);
		return valueCompare != 0 ? valueCompare : value.compareTo(o.value);
	}
}
