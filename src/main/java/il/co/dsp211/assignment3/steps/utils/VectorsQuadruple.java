package il.co.dsp211.assignment3.steps.utils;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class VectorsQuadruple implements WritableComparable<VectorsQuadruple>
{
	private LongWritable[] vector5;
	private DoubleWritable[] vector6, vector7, vector8;

	public VectorsQuadruple()
	{
	}

	public VectorsQuadruple(LongWritable[] vector5, DoubleWritable[] vector6, DoubleWritable[] vector7, DoubleWritable[] vector8)
	{
		this.vector5 = vector5;
		this.vector6 = vector6;
		this.vector7 = vector7;
		this.vector8 = vector8;
	}

	public LongWritable[] getVector5()
	{
		return vector5;
	}

	public DoubleWritable[] getVector6()
	{
		return vector6;
	}

	public DoubleWritable[] getVector7()
	{
		return vector7;
	}

	public DoubleWritable[] getVector8()
	{
		return vector8;
	}

	private static <T extends Comparable<? super T>> int compare(T[] a, T[] b) {
		if (a == b)
			return 0;
		// A null array is less than a non-null array
		if (a == null || b == null)
			return a == null ? -1 : 1;

		int length = Math.min(a.length, b.length);
		for (int i = 0; i < length; i++) {
			T oa = a[i];
			T ob = b[i];
			if (oa != ob) {
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
	public int compareTo(VectorsQuadruple o)
	{
		final int
				vector5Compare = compare(vector5, o.vector5),
				vector6Compare,
				vector7Compare;
		return vector5Compare != 0 ? vector5Compare :
		       (vector6Compare = compare(vector6, o.vector6)) != 0 ? vector6Compare :
		       (vector7Compare = compare(vector7, o.vector7)) != 0 ? vector7Compare :
		       compare(vector8, o.vector8);
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		new ArrayWritable(LongWritable.class, vector5).write(out);
		new ArrayWritable(DoubleWritable.class, vector6).write(out);
		new ArrayWritable(DoubleWritable.class, vector7).write(out);
		new ArrayWritable(DoubleWritable.class, vector8).write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		ArrayWritable temp = new ArrayWritable(LongWritable.class);

		temp.readFields(in);
		vector5 = (LongWritable[]) temp.get();

		temp = new ArrayWritable(DoubleWritable.class);
		temp.readFields(in);
		vector6 = (DoubleWritable[]) temp.get();

		temp.readFields(in);
		vector7 = (DoubleWritable[]) temp.get();

		temp.readFields(in);
		vector8 = (DoubleWritable[]) temp.get();
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (!(o instanceof VectorsQuadruple))
			return false;
		VectorsQuadruple that = (VectorsQuadruple) o;
		return Arrays.equals(vector5, that.vector5) &&
		       Arrays.equals(vector6, that.vector6) &&
		       Arrays.equals(vector7, that.vector7) &&
		       Arrays.equals(vector8, that.vector8);
	}

	@Override
	public int hashCode()
	{
		int result = Arrays.hashCode(vector5);
		result = 31 * result + Arrays.hashCode(vector6);
		result = 31 * result + Arrays.hashCode(vector7);
		result = 31 * result + Arrays.hashCode(vector8);
		return result;
	}

	private static void arraysString(StringBuilder b, Object[] a) {
		if (a == null)
		{
			b.append("null");
			return;
		}

		int iMax = a.length - 1;
		if (iMax == -1)
			return;

		for (int i = 0; ; i++) {
			b.append(a[i]);
			if (i == iMax)
				return;
			b.append("#");
		}
	}

	private static VectorsQuadruple of(String string)
	{
		final String[] values = string.split("🤠");
		
	}

	@Override
	public String toString()
	{
		final StringBuilder stringBuilder = new StringBuilder();
		arraysString(stringBuilder, vector5);
		stringBuilder.append("🤠");
		arraysString(stringBuilder, vector6);
		stringBuilder.append("🤠");
		arraysString(stringBuilder, vector7);
		stringBuilder.append("🤠");
		arraysString(stringBuilder, vector8);
		return stringBuilder.toString();
	}
}
