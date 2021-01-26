package il.co.dsp211.assignment3.steps.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StringDepLabelPair implements WritableComparable<StringDepLabelPair>
{
	private String word;
	private DepLabels depLabel;

	public StringDepLabelPair()
	{
	}

	public StringDepLabelPair(String word, DepLabels depLabel)
	{
		this.word = word;
		this.depLabel = depLabel;
	}

	public static StringDepLabelPair of(String string)
	{
		final String[] values = string.split("🤠");
		return new StringDepLabelPair(values[0], DepLabels.valueOf(values[1]));
	}

	public String getWord()
	{
		return word;
	}

	public DepLabels getDepLabel()
	{
		return depLabel;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (!(o instanceof StringDepLabelPair))
			return false;
		StringDepLabelPair that = (StringDepLabelPair) o;
		return word.equals(that.word) &&
		       depLabel == that.depLabel;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(word, depLabel);
	}

	@Override
	public String toString()
	{
		return word + "🤠" + depLabel;
	}

	@Override
	public int compareTo(StringDepLabelPair o)
	{
		final int wordCompare = word.compareTo(o.word);
		return wordCompare != 0 ? wordCompare : depLabel.compareTo(o.depLabel);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		Text.writeString(out, word);
		WritableUtils.writeEnum(out, depLabel);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		word = Text.readString(in);
		depLabel = WritableUtils.readEnum(in, DepLabels.class);
	}
}
