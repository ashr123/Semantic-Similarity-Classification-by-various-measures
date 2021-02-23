package il.co.dsp211.assignment3.steps.step1;

import weka.classifiers.Classifier;
import weka.classifiers.evaluation.Evaluation;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.core.Instances;
import weka.core.SerializationHelper;
import weka.core.converters.ConverterUtils.DataSource;

import java.io.InputStream;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;


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
		try
		{
			if (read.classIndex() == -1)
			{
				read.setClassIndex(read.numAttributes() - 1);
			}
		}
		catch (Exception ex)
		{
			Logger.getLogger(ModelGenerator.class.getName()).log(Level.SEVERE, null, ex);
		}

		return read;
	}

	public Classifier buildClassifier(Instances traindataset)
	{
		MultilayerPerceptron m = new MultilayerPerceptron();

		try
		{
			m.buildClassifier(traindataset);
		}
		catch (Exception ex)
		{
			Logger.getLogger(ModelGenerator.class.getName()).log(Level.SEVERE, null, ex);
		}
		return m;
	}

	public String evaluateModel(Classifier model, Instances traindataset, Instances testdataset)
	{
		Evaluation eval = null;
		try
		{
			// Evaluate classifier with test dataset
			eval = new Evaluation(traindataset);

			eval.crossValidateModel(model, testdataset, 10, new Random());
//			eval.evaluateModel(model, testdataset);
		}
		catch (Exception ex)
		{
			Logger.getLogger(ModelGenerator.class.getName()).log(Level.SEVERE, null, ex);
		}
		return new StringBuilder(eval.toSummaryString(true)).append('\n')
				.append("Recall:\t").append(eval.recall(1)).append('\n')
				.append("Precision:\t").append(eval.precision(1)).append('\n')
				.append("F1:\t").append(eval.fMeasure(1)).append('\n')
				.toString();
	}

	public void saveModel(Classifier model, String modelpath)
	{
		try
		{
			SerializationHelper.write(modelpath, model);
		}
		catch (Exception ex)
		{
			Logger.getLogger(ModelGenerator.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

}
