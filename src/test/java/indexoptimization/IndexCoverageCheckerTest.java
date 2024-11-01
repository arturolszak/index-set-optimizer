package indexoptimization;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class IndexCoverageCheckerTest {

    @Test
    public void test_index_coverage_checker() {
        IndexCoverageChecker checker = new IndexCoverageChecker();
        String[] inputIndexStrings = {
            "{{1,0}}",
            "{{0,2}{4,5}{6}}",
            "{{1,2,0}}",
            "{{2,0}}",
            "{{3}{0,1}}",
            "{{1,2}{0}}"
        };

        String[] coveringIndexStrings = {
            "{{0,1}{2}}",
            "{{0,2}}",
            "{{1,2}{0}}"
        };

        List<Index> indexes = parseInputStrings(inputIndexStrings);
        List<Index> coveringIndexes = parseInputStrings(coveringIndexStrings);

        Pair<Map<Index, List<Index>>, List<Index>> checkRes = checker.checkIndexCoverage(indexes, coveringIndexes);
        Assertions.assertEquals(4, checkRes.getLeft().size());
        Assertions.assertEquals(2, checkRes.getRight().size());
    }

    private List<Index> parseInputStrings(String[] inputIndexStrings) {
        return Arrays.stream(inputIndexStrings)
            .map(Index::parseIndex)
            .collect(Collectors.toList());
    }

}
