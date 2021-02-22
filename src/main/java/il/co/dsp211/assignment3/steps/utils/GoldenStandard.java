package il.co.dsp211.assignment3.steps.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

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
						goldenStandard = bufferedReader.lines()
								.map(line -> line.split("\t"))
								.collect(Collectors.groupingBy(strings -> strings[0],
										Collectors.toMap(strings -> strings[1], strings -> Boolean.valueOf(strings[2]))));
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
								.flatMap(strings -> Stream.of(strings[0], strings[1]))
								.collect(Collectors.toSet());
					}
			}
		return goldenStandardSet;
	}
}
