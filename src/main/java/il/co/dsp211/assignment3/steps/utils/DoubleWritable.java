package il.co.dsp211.assignment3.steps.utils;

public class DoubleWritable extends org.apache.hadoop.io.DoubleWritable
{
	public DoubleWritable()
	{
		super();
	}

	public DoubleWritable(double value)
	{
		super(value);
	}

	@Override
	public String toString()
	{
		return Double.isNaN(get()) ? "?" : super.toString();
	}
}
