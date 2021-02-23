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

	private Attribute freq_distManhattan;
	private Attribute freq_distEuclidean;
	private Attribute freq_simCosine;
	private Attribute freq_simJaccard;
	private Attribute freq_simDice;
	private Attribute freq_simJS;

	private Attribute prob_distManhattan;
	private Attribute prob_distEuclidean;
	private Attribute prob_simCosine;
	private Attribute prob_simJaccard;
	private Attribute prob_simDice;
	private Attribute prob_simJS;

	private Attribute PMI_distManhattan;
	private Attribute PMI_distEuclidean;
	private Attribute PMI_simCosine;
	private Attribute PMI_simJaccard;
	private Attribute PMI_simDice;
	private Attribute PMI_simJS;

	private Attribute ttest_distManhattan;
	private Attribute ttest_distEuclidean;
	private Attribute ttest_simCosine;
	private Attribute ttest_simJaccard;
	private Attribute ttest_simDice;
	private Attribute ttest_simJS;

	private ArrayList<Attribute> attributes;
	private ArrayList<String> classVal;
	private Instances dataRaw;


	public ModelClassifier() {
		freq_distManhattan = new Attribute("freq_distManhattan");
		freq_distEuclidean = new Attribute("freq_distEuclidean");
		freq_simCosine = new Attribute("freq_simCosine");
		freq_simJaccard = new Attribute("freq_simJaccard");
		freq_simDice = new Attribute("freq_simDice");
		freq_simJS = new Attribute("freq_simJS");

		prob_distManhattan = new Attribute("prob_distManhattan");
		prob_distEuclidean = new Attribute("prob_distEuclidean");
		prob_simCosine = new Attribute("prob_simCosine");
		prob_simJaccard = new Attribute("prob_simJaccard");
		prob_simDice = new Attribute("prob_simDice");
		prob_simJS = new Attribute("prob_simJS");

		PMI_distManhattan = new Attribute("PMI_distManhattan");
		PMI_distEuclidean = new Attribute("PMI_distEuclidean");
		PMI_simCosine = new Attribute("PMI_simCosine");
		PMI_simJaccard = new Attribute("PMI_simJaccard");
		PMI_simDice = new Attribute("PMI_simDice");
		PMI_simJS = new Attribute("PMI_simJS");

		ttest_distManhattan = new Attribute("ttest_distManhattan");
		ttest_distEuclidean = new Attribute("ttest_distEuclidean");
		ttest_simCosine = new Attribute("ttest_simCosine");
		ttest_simJaccard = new Attribute("ttest_simJaccard");
		ttest_simDice = new Attribute("ttest_simDice");
		ttest_simJS = new Attribute("ttest_simJS");

		attributes = new ArrayList<>();
		classVal = new ArrayList<>();
		classVal.add("true");
		classVal.add("false");

		attributes.add(freq_distManhattan);
		attributes.add(freq_distEuclidean);

		attributes.add(new Attribute("class", classVal));
		dataRaw = new Instances("TestInstances", attributes, 0);
		dataRaw.setClassIndex(dataRaw.numAttributes() - 1);
	}


	public Instances createInstance(double petallength, double petalwidth, double result) {
		dataRaw.clear();
		double[] instanceValue1 = new double[]{petallength, petalwidth, 0};
		dataRaw.add(new DenseInstance(1.0, instanceValue1));
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
