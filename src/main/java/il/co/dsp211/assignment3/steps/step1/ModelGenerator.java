package il.co.dsp211.assignment3.steps.step1;

import weka.classifiers.Classifier;
import weka.classifiers.evaluation.Evaluation;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;

import java.io.InputStream;
import java.util.Random;


public class ModelGenerator
{

	public Instances loadDataset(String path) throws Exception
	{
		return getInstances(DataSource.read(path));
	}

	public Instances loadDataset(InputStream path) throws Exception
	{
		return getInstances(DataSource.read(path));
	}

	private Instances getInstances(Instances read)
	{
		read.setClassIndex(read.numAttributes() - 1);
		return read;
	}

	public Classifier buildClassifier(Instances traindataset) throws Exception
	{
		final MultilayerPerceptron m = new MultilayerPerceptron();
		m.buildClassifier(traindataset);
		return m;
	}

	public String evaluateModel(Classifier model, Instances traindataset, Instances testdataset) throws Exception
	{
		final Evaluation eval = new Evaluation(traindataset);

		eval.crossValidateModel(model, testdataset, 10, new Random());
		return new StringBuilder(eval.toSummaryString(true)).append('\n')
				.append(eval.toClassDetailsString())
				.toString();
	}

}
