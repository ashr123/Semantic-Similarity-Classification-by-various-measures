package il.co.dsp211.assignment3.steps.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StringStringPair implements WritableComparable<StringStringPair>
{
	private String word, depLabel;

	public StringStringPair()
	{
	}

	public StringStringPair(String word, String depLabel)
	{
		this.word = word;
		this.depLabel = depLabel;
	}

	public static StringStringPair of(String string)
	{
		final String[] values = string.split("#");
		return new StringStringPair(values[0], values[1]);
	}

	public String getWord()
	{
		return word;
	}

	public String getDepLabel()
	{
		return depLabel;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (!(o instanceof StringStringPair))
			return false;
		StringStringPair that = (StringStringPair) o;
		return word.equals(that.word) &&
		       depLabel.equals(that.depLabel);
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(word, depLabel);
	}

	@Override
	public String toString()
	{
		return word + "#" + depLabel;
	}

	@Override
	public int compareTo(StringStringPair o)
	{
		final int wordCompare = word.compareTo(o.word);
		return wordCompare != 0 ? wordCompare : depLabel.compareTo(o.depLabel);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		Text.writeString(out, word);
		Text.writeString(out, depLabel);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		word = Text.readString(in);
		depLabel = Text.readString(in);
	}
}
