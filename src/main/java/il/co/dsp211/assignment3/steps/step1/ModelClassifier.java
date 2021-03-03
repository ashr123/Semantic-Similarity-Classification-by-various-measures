package il.co.dsp211.assignment3.steps.step1;

import weka.classifiers.Classifier;
import weka.core.DenseInstance;
import weka.core.Instance;

import java.util.ArrayList;

public class ModelClassifier
{
	private final ArrayList<String> classVal;
//	private final Instances dataRaw;


	public ModelClassifier()
	{
//		String[] a = {"freq", "prob", "PMI", "ttest"};
//		String[] b = {"distManhattan", "distEuclidean", "simCosine", "simJaccard", "simDice", "simJS"};
//		ArrayList<Attribute> attributes = new ArrayList<>(a.length * b.length);
//		for (String value : a)
//		{
//			for (String s : b)
//			{
//				attributes.add(new Attribute(value + "_" + s));
//			}
//		}

		classVal = new ArrayList<>(2);

		classVal.add("true");
		classVal.add("false");

//		attributes.add(new Attribute("class", classVal));
//		dataRaw = new Instances("TestInstances", attributes, 0);
//		dataRaw.setClassIndex(dataRaw.numAttributes() - 1);
	}


	// Get 25 Dimension array (24-vector + result=0)
	public Instance createInstance(double... arr)
	{
//		dataRaw.clear();
//		dataRaw.add(new DenseInstance(1.0, arr));
//		return dataRaw;
		return new DenseInstance(1.0, arr);
	}


//	public String classify(Instances insts, String path)
//	{
//		String result = "Not classified!!";
//		Classifier cls = null;
//		try
//		{
//			cls = (Classifier) SerializationHelper.read(path);
//			result = classVal.get((int) cls.classifyInstance(insts.firstInstance()));
//		}
//		catch (Exception ex)
//		{
//			Logger.getLogger(ModelClassifier.class.getName()).log(Level.SEVERE, null, ex);
//		}
//		return result;
//	}

	public String classify(Classifier cls, Instance inst) throws Exception
	{
		return classVal.get((int) cls.classifyInstance(inst));
	}


//	public Instances getInstance()
//	{
//		return dataRaw;
//	}
}
