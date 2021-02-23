package il.co.dsp211.assignment3.steps.utils;

import opennlp.tools.stemmer.PorterStemmer;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GoldenStandard
{
	private static Map<String, Map<String, Boolean>> goldenStandard;
	private static Set<String> goldenStandardSet;

	private GoldenStandard()
	{
	}

	public static Map<String, Map<String, Boolean>> getGoldenStandard(Configuration conf) throws IOException
	{
		if (goldenStandard == null)
			synchronized (GoldenStandard.class)
			{
				if (goldenStandard == null)
					try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream(conf.get("goldenStandardFileName")))))
					{
						goldenStandard = bufferedReader.lines().parallel()
								.map(line -> line.split("\t"))
								.collect(Collectors.groupingBy(strings -> new PorterStemmer().stem(strings[0]),
										Collectors.toMap(strings -> new PorterStemmer().stem(strings[1]), strings -> Boolean.valueOf(strings[2]), (b1, b2) -> b1 && b2)));
					}
			}
		return goldenStandard;
	}

	public static Set<String> readGoldenStandardToSet(Configuration conf) throws IOException
	{
		if (goldenStandardSet == null)
			synchronized (GoldenStandard.class)
			{
				if (goldenStandardSet == null)
					try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream(conf.get("goldenStandardFileName")))))
					{
						goldenStandardSet = bufferedReader.lines().parallel()
								.map(line -> line.split("\t"))
								.flatMap(strings -> Stream.of(new PorterStemmer().stem(strings[0]), new PorterStemmer().stem(strings[1])))
								.collect(Collectors.toSet());
					}
			}
		return goldenStandardSet;
	}
}
