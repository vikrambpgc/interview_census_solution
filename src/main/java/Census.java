import org.checkerframework.checker.units.qual.A;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implement the two methods below. We expect this class to be stateless and thread safe.
 */
public class Census {
    /**
     * Number of cores in the current machine.
     */
    private static final int CORES = Runtime.getRuntime().availableProcessors();

    /**
     * Output format expected by our tests.
     */
    public static final String OUTPUT_FORMAT = "%d:%d=%d"; // Position:Age=Total

    /**
     * Factory for iterators.
     */
    private final Function<String, Census.AgeInputIterator> iteratorFactory;

    /**
     * Creates a new Census calculator.
     *
     * @param iteratorFactory factory for the iterators.
     */
    public Census(Function<String, Census.AgeInputIterator> iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }

    /**
     * Given one region name, call {@link #iteratorFactory} to get an iterator for this region and return
     * the 3 most common ages in the format specified by {@link #OUTPUT_FORMAT}.
     */
    public String[] top3Ages(String region) {
            Map<Integer, Integer> frequencyMap = getFrequencyMap(region);
            Map<Integer, Integer> sortedFrequencyMap = sortByMapValue(frequencyMap);

            return getTop3EntriesInOutputFormat(sortedFrequencyMap);

            //        In the example below, the top three are ages 10, 15 and 12
            //        return new String[]{
            //                String.format(OUTPUT_FORMAT, 1, 10, 38),
            //                String.format(OUTPUT_FORMAT, 2, 15, 35),
            //                String.format(OUTPUT_FORMAT, 3, 12, 30)
            //        };

            // throw new UnsupportedOperationException();
    }

    /**
     * Given a list of region names, call {@link #iteratorFactory} to get an iterator for each region and return
     * the 3 most common ages across all regions in the format specified by {@link #OUTPUT_FORMAT}.
     * We expect you to make use of all cores in the machine, specified by {@link #CORES).
     */
    public String[] top3Ages(List<String> regionNames) {
        ForkJoinPool forkJoinPool = null;
        Map<Integer, Integer> finalMap = null;
        try {
            // Custom Fork Join pool.
            forkJoinPool = new ForkJoinPool(CORES);
            finalMap = regionNames.parallelStream()
                    .map(this::getFrequencyMap)
                    .flatMap(map -> map.entrySet().stream())
                    .collect(Collectors.groupingBy(Map.Entry::getKey,
                            Collectors.summingInt(Map.Entry::getValue)));
        } finally {
            if (forkJoinPool != null) {
                forkJoinPool.shutdown();
            }
        }

        Map<Integer, Integer> sortedFrequencyMap = sortByMapValue(finalMap);

        return getTop3EntriesInOutputFormat(sortedFrequencyMap);
        //        In the example below, the top three are ages 10, 15 and 12
        //        return new String[]{
        //                String.format(OUTPUT_FORMAT, 1, 10, 38),
        //                String.format(OUTPUT_FORMAT, 2, 15, 35),
        //                String.format(OUTPUT_FORMAT, 3, 12, 30)
        //        };

        //throw new UnsupportedOperationException();
    }

    /**
     *
     * @param region
     * @return Age to Frequency/Total Map for a region
     */
    private Map<Integer, Integer> getFrequencyMap(String region) {
        Map<Integer, Integer> frequencyMap = new HashMap<>();
        try (AgeInputIterator iterator = iteratorFactory.apply(region)) {
            if (iterator == null) return frequencyMap;
            while (iterator.hasNext()) {
                Integer key = iterator.next();
                if (key < 0) continue;
                frequencyMap.put(key, frequencyMap.getOrDefault(key, 0) + 1);
            }
        } catch (Exception e) {
            //Do nothing
        }

        return frequencyMap;
    }

    /**
     * Sort the given map by value in decreasing order
     * @param inputMap
     * @return
     */
    private Map<Integer, Integer> sortByMapValue(Map<Integer, Integer> inputMap) {
        return inputMap.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue())) // descending
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }

    /**
     * Returns the top 3 entries in a Map
     * If two entries have same Values, they have the same rank/position
     * @param sortedMap
     * @return
     */
    private String[] getTop3EntriesInOutputFormat(Map<Integer, Integer> sortedMap) {
        List<String> resultList = new ArrayList<>();
        int position = 0;
        int prevTotal = -1;
        for (Map.Entry<Integer, Integer> entry : sortedMap.entrySet()) {
            if (entry.getValue() != prevTotal) {
                position++;
            }
            if (position >= 4) break; // Only up to top 3 positions need to be returned.
            resultList.add(String.format(OUTPUT_FORMAT, position, entry.getKey(), entry.getValue()));

            prevTotal = entry.getValue();
        }
        return resultList.toArray(new String[0]);
    }


    /**
     * Implementations of this interface will return ages on call to {@link Iterator#next()}. They may open resources
     * when being instantiated created.
     */
    public interface AgeInputIterator extends Iterator<Integer>, Closeable {
    }
}
