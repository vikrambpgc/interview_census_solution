import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
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
        try (AgeInputIterator iterator = iteratorFactory.apply(region)) {
            Map<Integer, Integer> frequencyMap = new HashMap<>();
            while (iterator.hasNext()) {
                Integer key = iterator.next();
                if (key < 0) continue;
                frequencyMap.put(key, frequencyMap.getOrDefault(key, 0) + 1);
            }

            Map<Integer, Integer> sortedFrequencyMap = frequencyMap.entrySet().stream()
                    .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue())) // descending
                    .limit(3).collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (e1, e2) -> e1,
                            LinkedHashMap::new
                    ));

            String[] result = new String[3];
            int position = 1;
            for (Map.Entry<Integer, Integer> entry : sortedFrequencyMap.entrySet()) {
                result[position - 1] = String.format(OUTPUT_FORMAT, position, entry.getKey(), entry.getValue());
                position++;
            }
            return result;
        } catch (IOException e) {
            return new String[]{};
        }


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
        System.out.println(regionNames);
        Map<Integer, Integer> finalMap = regionNames.stream().map((regionName) -> {
            System.out.println(regionName);
            Map<Integer, Integer> frequencyMap = new HashMap<>();
            try (AgeInputIterator iterator = iteratorFactory.apply(regionName)) {
                while (iterator.hasNext()) {
                    Integer key = iterator.next();
                    if (key < 0) continue;
                    frequencyMap.put(key, frequencyMap.getOrDefault(key, 0) + 1);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return frequencyMap;
        }).reduce(new HashMap<>(),
                (map1, map2) -> {
                    map2.forEach((key, value) -> map1.merge(key, value, Integer::sum));
                    return map1;
                },
                (map1, map2) -> {
                    map2.forEach((key, value) -> map1.merge(key, value, Integer::sum));
                    return map1;
                });

        Map<Integer, Integer> sortedFrequencyMap = finalMap.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue())) // descending
                .limit(3).collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));

        String[] result = new String[3];
        int position = 1;
        for (Map.Entry<Integer, Integer> entry : sortedFrequencyMap.entrySet()) {
            result[position - 1] = String.format(OUTPUT_FORMAT, position, entry.getKey(), entry.getValue());
            position++;
        }
        return result;
//        In the example below, the top three are ages 10, 15 and 12
//        return new String[]{
//                String.format(OUTPUT_FORMAT, 1, 10, 38),
//                String.format(OUTPUT_FORMAT, 2, 15, 35),
//                String.format(OUTPUT_FORMAT, 3, 12, 30)
//        };

        //throw new UnsupportedOperationException();
    }


    /**
     * Implementations of this interface will return ages on call to {@link Iterator#next()}. They may open resources
     * when being instantiated created.
     */
    public interface AgeInputIterator extends Iterator<Integer>, Closeable {
    }
}
