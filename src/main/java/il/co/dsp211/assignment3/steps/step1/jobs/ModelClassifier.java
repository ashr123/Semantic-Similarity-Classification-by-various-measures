package il.co.dsp211.assignment3.steps.step1.jobs;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import weka.classifiers.Classifier;
import weka.classifiers.functions.MultilayerPerceptron;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instances;
import weka.core.SerializationHelper;


public class ModelClassifier {

	private ArrayList<Attribute> attributes;
	private ArrayList<String> classVal;
	private Instances dataRaw;


	public ModelClassifier() {
		attributes = new ArrayList<>();

		String[] a = {"freq", "prob", "PMI", "ttest"};
		String[] b = {"distManhattan", "distEuclidean", "simCosine", "simJaccard", "simDice", "simJS"};
		for (int i = 0; i < a.length; i++) {
			for (int j = 0; j < b.length; j++) {
				attributes.add(new Attribute(a[i] + "_" + b[j]));
			}
		}

		classVal = new ArrayList<>();
		classVal.add("true");
		classVal.add("false");

		attributes.add(new Attribute("class", classVal));
		dataRaw = new Instances("TestInstances", attributes, 0);
		dataRaw.setClassIndex(dataRaw.numAttributes() - 1);
	}


	public Instances createInstance(double v1, double v2, double v3, double v4, double v5, double v6, double v7, double v8, double v9, double v10, double v11, double v12, double v13, double v14, double v15, double v16, double v17, double v18, double v19, double v20, double v21, double v22, double v23, double v24, double result) {
		dataRaw.clear();
		dataRaw.add(new DenseInstance(1.0, new double[]{v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21, v22, v23, v24, 0}));
		return dataRaw;
	}


	public String classifiy(Instances insts, String path) {
		String result = "Not classified!!";
		Classifier cls = null;
		try {
			cls = (MultilayerPerceptron) SerializationHelper.read(path);
			result = (String) classVal.get((int) cls.classifyInstance(insts.firstInstance()));
		} catch (Exception ex) {
			Logger.getLogger(ModelClassifier.class.getName()).log(Level.SEVERE, null, ex);
		}
		return result;
	}


	public Instances getInstance() {
		return dataRaw;
	}


}
