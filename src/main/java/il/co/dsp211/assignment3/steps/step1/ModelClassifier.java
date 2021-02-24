package il.co.dsp211.assignment3.steps.step1;

import weka.classifiers.Classifier;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instances;
import weka.core.SerializationHelper;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ModelClassifier
{
	private final ArrayList<String> classVal;
	private final Instances dataRaw;


	public ModelClassifier()
	{

		String[] a = {"freq", "prob", "PMI", "ttest"};
		String[] b = {"distManhattan", "distEuclidean", "simCosine", "simJaccard", "simDice", "simJS"};
		ArrayList<Attribute> attributes = new ArrayList<>(a.length * b.length);
		for (String value : a)
		{
			for (String s : b)
			{
				attributes.add(new Attribute(value + "_" + s));
			}
		}

		classVal = new ArrayList<>(2);

		classVal.add("true");
		classVal.add("false");

		attributes.add(new Attribute("class", classVal));
		dataRaw = new Instances("TestInstances", attributes, 0);
		dataRaw.setClassIndex(dataRaw.numAttributes() - 1);
	}


	// Get 25 Dimension array (24-vector + result=0)
	public Instances createInstance(double... arr)
	{
		dataRaw.clear();
		dataRaw.add(new DenseInstance(1.0, arr));
		return dataRaw;
	}


	public String classifiy(Instances insts, String path)
	{
		String result = "Not classified!!";
		Classifier cls = null;
		try
		{
			cls = (MultilayerPerceptron) SerializationHelper.read(path);
			result = classVal.get((int) cls.classifyInstance(insts.firstInstance()));
		}
		catch (Exception ex)
		{
			Logger.getLogger(ModelClassifier.class.getName()).log(Level.SEVERE, null, ex);
		}
		return result;
	}


	public Instances getInstance()
	{
		return dataRaw;
	}


}
