package indexoptimization;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.contains;

class IndexOptimizerTest {
    @Test
    public void testParseAndToString() {
        String indexStr = "{{a,d,f,g,j,n,r,t,z}}";
        Index index = Index.parseIndex(indexStr);
        System.out.println(index.toString());
    }

    @Test
    public void testParseAndToStringWithGroups() {
        String indexStr = "{{a,d}{f}{g,j}{n,r,t,z}}";
        Index index = Index.parseIndex(indexStr);
        System.out.println(index.toString());
    }

    @Test
    public void testWithSmallestIndexSetStrategy() {

        // Arrange
        String[] inputIndexStrings = {
                "{{a,d,f,g,j,n,r,t,z}}",
                "{{d,g,r}}",
                "{{a,z}}",
                "{{b,r}}",
                "{{g}}"
        };

        List<Index> indexes = Arrays.stream(inputIndexStrings)
                .map(s -> Index.parseIndex(s))
                .collect(Collectors.toList());

        // Act
        IndexOptimizer indexOptimizer = new IndexOptimizer(new SmallestIndexListSelectionStrategy());
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);

        // Assert
        for (Index optimizedIndex : optimizedIndexes) {
            System.out.println(optimizedIndex.toStringSorted());
        }

        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(is -> is.toStringSorted())
                .collect(Collectors.toList());
        Assertions.assertEquals(3, outputIndexStrings.size());
        Assertions.assertTrue(outputIndexStrings.contains("{{g}{d,r}{a,f,j,n,t,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{a,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{b,r}}"));
    }

    @Test
    public void testWithSmallestIndexSetStrategy2() {

        // Arrange
        String[] inputIndexStrings = {
                "{{x,y,z}}",
                "{{z,y}}",
                "{{z,x}}",
                "{{x}}"
        };

        List<Index> indexes = Arrays.stream(inputIndexStrings)
                .map(s -> Index.parseIndex(s))
                .collect(Collectors.toList());

        // Act
        IndexOptimizer indexOptimizer = new IndexOptimizer(new SmallestIndexListSelectionStrategy());
        indexOptimizer.setMemoize(false);
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);

        // Assert
        for (Index optimizedIndex : optimizedIndexes) {
            System.out.println(optimizedIndex.toStringSorted());
        }

        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(is -> is.toString())
                .collect(Collectors.toList());
        Assertions.assertEquals(2, outputIndexStrings.size());
        Assertions.assertTrue(outputIndexStrings.contains("{{y,z}{x}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{x}{z}}"));
    }

    @Test
    public void testWithSmallestIndexSetStrategyWithoutMemoization() {

        // Arrange
        String[] inputIndexStrings = {
                "{{a,d,f,g,j,n,r,t,z}}",
                "{{d,g,r}}",
                "{{a,z}}",
                "{{b,r}}",
                "{{g}}"
        };

        List<Index> indexes = Arrays.stream(inputIndexStrings)
                .map(s -> Index.parseIndex(s))
                .collect(Collectors.toList());

        // Act
        IndexOptimizer indexOptimizer = new IndexOptimizer(new SmallestIndexListSelectionStrategy());
        indexOptimizer.setMemoize(false);
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);

        // Assert
        for (Index optimizedIndex : optimizedIndexes) {
            System.out.println(optimizedIndex.toStringSorted());
        }

        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(is -> is.toStringSorted())
                .collect(Collectors.toList());
        Assertions.assertEquals(3, outputIndexStrings.size());
        Assertions.assertTrue(outputIndexStrings.contains("{{g}{d,r}{a,f,j,n,t,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{a,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{b,r}}"));
    }

    @Disabled
    @Test
    public void compareTimesWithAndWithoutMemoization() {

        // Arrange
        String[] inputIndexStrings = {
                "{{a,d,f,g,j,n,r,t,z}}",
                "{{d,g,r}}",
                "{{a,z}}",
                "{{b,r}}",
                "{{g}}"
        };

        List<Index> indexes = Arrays.stream(inputIndexStrings)
                .map(s -> Index.parseIndex(s))
                .collect(Collectors.toList());

        // Act
        IndexOptimizer indexOptimizer;
        long t1, t2;

        indexOptimizer = new IndexOptimizer(new SmallestIndexListSelectionStrategy());
        indexOptimizer.setMemoize(false);
        t1 = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            indexOptimizer.optimizeIndexes(indexes);
        }
        t2 = System.currentTimeMillis();
        System.out.println("Execution time (without memoization): " + (t2 - t1));

        indexOptimizer = new IndexOptimizer(new SmallestIndexListSelectionStrategy());
        indexOptimizer.setMemoize(true);
        t1 = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            indexOptimizer.optimizeIndexes(indexes);
        }
        t2 = System.currentTimeMillis();
        System.out.println("Execution time (with memoization):    " + (t2 - t1));



    }

    @Test
    public void testWithLargestIndexSetStrategy() {

        // Arrange
        String[] inputIndexStrings = {
                "{{a,d,f,g,j,n,r,t,z}}",
                "{{d,g,r}}",
                "{{a,z}}",
                "{{b,r}}",
                "{{g}}"
        };

        List<Index> indexes = Arrays.stream(inputIndexStrings)
                .map(s -> Index.parseIndex(s))
                .collect(Collectors.toList());

        // Act
        IndexOptimizer indexOptimizer = new IndexOptimizer(new LargestIndexListSelectionStrategy());
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);

        // Assert
        for (Index optimizedIndex : optimizedIndexes) {
            System.out.println(optimizedIndex.toStringSorted());
        }

        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(is -> is.toStringSorted())
                .collect(Collectors.toList());
        Assertions.assertEquals(5, outputIndexStrings.size());
        Assertions.assertTrue(outputIndexStrings.contains("{{a,d,f,g,j,n,r,t,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{d,g,r}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{a,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{b,r}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{g}}"));
    }

    @Test
    @DisplayName("duplicates are removed")
    public void testWithSmallestIndexSetStrategyWithoutMemoization2() {
        // Arrange
        List<Index> indexes = parseInputStrings(new String[]{
                "{{a,b}}",
                "{{a,b}}"
        });

        // Act
        List<Index> optimizedIndexes = createOptimizer().optimizeIndexes(indexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        assertThat(outputIndexStrings, hasSize(1));
        assertThat(outputIndexStrings, contains("{{a,b}}"));
    }

    @Test
    @DisplayName("duplicates are removed (reversed order)")
    public void testWithSmallestIndexSetStrategyWithoutMemoization3() {
        // Arrange
        List<Index> indexes = parseInputStrings(new String[]{
                "{{a,b}}",
                "{{b,a}}"
        });

        // Act
        List<Index> optimizedIndexes = createOptimizer().optimizeIndexes(indexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        assertThat(outputIndexStrings, hasSize(1));
        assertThat(outputIndexStrings, contains("{{a,b}}"));
    }

    private List<Index> parseInputStrings(String[] inputIndexStrings) {
        return Arrays.stream(inputIndexStrings)
                .map(Index::parseIndex)
                .collect(Collectors.toList());
    }

    private IndexOptimizer createOptimizer() {
        IndexOptimizer indexOptimizer = new IndexOptimizer(new SmallestIndexListSelectionStrategy());
        indexOptimizer.setMemoize(false);
        return indexOptimizer;
    }

}