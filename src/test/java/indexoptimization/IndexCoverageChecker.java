package indexoptimization;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class IndexCoverageChecker {

    public Pair<List<Pair<Index, List<Index>>>, List<Index>> checkIndexCoverage(List<Index> indexes,
                                                                                List<Index> coveringIndexes) {
        List<Pair<Index, List<Index>>> covered = new ArrayList<>();
        List<Index> notCovered = new ArrayList<>();
        for (Index index : indexes) {
            List<Index> foundCoveringIndexes = checkIndexCoverage(index, coveringIndexes);
            if (!foundCoveringIndexes.isEmpty()) {
                covered.add(Pair.of(index, foundCoveringIndexes));
            } else {
                notCovered.add(index);
            }
        }
        return Pair.of(covered, notCovered);
    }

    private List<Index> checkIndexCoverage(Index index, List<Index> coveringIndexes) {
        List<Index> foundCoveringIndexes = new ArrayList<>();
        for (Index coveringIndex : coveringIndexes) {
            if (covers(index, coveringIndex)) {
                foundCoveringIndexes.add(coveringIndex);
            }
        }
        return foundCoveringIndexes;
    }

    private boolean covers(Index index, Index coveringIndex) {

        LinkedHashMap<String, Boolean> coveredFields = new LinkedHashMap<>();
        //TODO consider all permutations of the indexes
        // maybe only important permutations in the last covering field set

        for (IndexFieldSet fieldSet : coveringIndex.getFieldSets()) {
            for (IndexField field : fieldSet.getFields()) {
                coveredFields.put(field.getName(), false);
            }
        }

        for (IndexFieldSet fieldSet : index.getFieldSets()) {
            for (IndexField field : fieldSet.getFields()) {
                if (coveredFields.containsKey(field.getName())) {
                    coveredFields.put(field.getName(), true);
                } else {
                    return false;
                }
            }
        }

        int i = 0;
        for (Map.Entry<String, Boolean> coveredField : coveredFields.entrySet()) {
            if (i == index.getLength()) {
                return true;
            }
            if (!coveredField.getValue()) {
                return false;
            }
            i++;
        }

        return true;
    }

}
