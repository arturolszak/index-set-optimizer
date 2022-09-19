package indexoptimization;

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class IndexOptimizer {

    public static final boolean SHOULD_CONSIDER_SKIPPED_OPTIMIZATIONS = true;
    private boolean memoize = true;
    private final IndexListSelectionStrategy indexListSelectionStrategy;
    private final HashMap<String, List<Index>> optimizedIndexesMemoizer = new HashMap<>();


    public IndexOptimizer() {
        this.indexListSelectionStrategy = new SmallestIndexListSelectionStrategy();
    }

    public IndexOptimizer(IndexListSelectionStrategy indexListSelectionStrategy) {
        this.indexListSelectionStrategy = indexListSelectionStrategy;
    }

    public List<Index> optimizeIndexes(List<Index> indexes) {
        List<Index> indexesSorted = indexes.stream()
            .sorted(Comparator.comparing(ind -> ((Index)ind).getLength()).reversed())
            .collect(Collectors.toList());

        return optimizeSortedIndexes(indexesSorted);
    }

    private List<Index> optimizeSortedIndexes(List<Index> indexes) {
        List<Pair<Index, Index>> containedContainingIndexPairs = calculateContainedContainingIndexPairs(indexes);
        if (!containedContainingIndexPairs.isEmpty()) {
            return optimizeIndexesRecursive(indexes, containedContainingIndexPairs);
        } else {
            return indexes;
        }
    }

    private List<Index> optimizeIndexesRecursive(List<Index> indexes, List<Pair<Index, Index>> containedContainingIndexPairs) {
        String s = indexes.toString() + "/" + containedContainingIndexPairs.toString();
        if (memoize) {
            List<Index> optimizedIndexes = optimizedIndexesMemoizer.get(s);
            if (optimizedIndexes != null) {
                return optimizedIndexes;
            }
        }

        List<Index> indices = doOptimizeIndexesRecursive(indexes, containedContainingIndexPairs);

        if (memoize) {
            optimizedIndexesMemoizer.put(s, indices);
        }
        return indices;
    }

    private List<Index> doOptimizeIndexesRecursive(List<Index> indexes, List<Pair<Index, Index>> containedContainingIndexPairs) {
        List<List<Index>> newIndexListCandidates = new ArrayList<>();

        //remove one by one from contained indexes and proceed recursively with remaining contained indexes
        for (int i = 0; i < containedContainingIndexPairs.size(); i++) {
            Pair<Index, Index> cc = containedContainingIndexPairs.get(i);
            Index containedIndex = cc.getLeft();
            Index containingIndex = cc.getRight();

            //TODO:
            // change the pairs list to be a hash set - make it faster to generate the remaining pairs
            // for now implemented it like this, so the execution tree is deterministic

            List<Index> indexesAfterRemovingOne = removeIndex(containedIndex, indexes);
            Index constrainedContainingIndex = addConstraintsToContainingIndex(containedIndex, containingIndex);
            List<Index> indexesAfterRemovingOneAndConstraining = replaceContainingWithConstrained(
                                                                                        containingIndex,
                                                                                        constrainedContainingIndex,
                                                                                        indexesAfterRemovingOne);
            List<Pair<Index, Index>> remainingCcPairs = getRemainingCcPairs(containedContainingIndexPairs,
                                                                            i,
                                                                            constrainedContainingIndex);
            if (! remainingCcPairs.isEmpty()) {
                List<Index> indexesAfterRecursiveRemoval = optimizeIndexesRecursive(indexesAfterRemovingOneAndConstraining, remainingCcPairs);
                newIndexListCandidates.add(indexesAfterRecursiveRemoval);
            } else {
                newIndexListCandidates.add(indexesAfterRemovingOneAndConstraining);
            }

            if (SHOULD_CONSIDER_SKIPPED_OPTIMIZATIONS) {
                List<Pair<Index, Index>> remainingCcPairsAfterSkipping = getRemainingCcPairsForSkippingOptimization(containedContainingIndexPairs, i);
                if (! remainingCcPairsAfterSkipping.isEmpty()) {
                    List<Index> indexesAfterRecursiveRemoval = optimizeIndexesRecursive(indexes, remainingCcPairsAfterSkipping);
                    newIndexListCandidates.add(indexesAfterRecursiveRemoval);
                } else {
                    newIndexListCandidates.add(indexes);
                }
            }

        }

        //chose the best according to the chosen strategy, and return it
        return choseBestIndexes(newIndexListCandidates);
    }

    private List<Index> removeIndex(Index contained, List<Index> indexes) {
        List<Index> newList = new ArrayList<>(indexes.size() - 1);
        for (Index index : indexes) {
            if (index.equals(contained)) {
                //skip (remove)
            } else {
                newList.add(index);
            }
        }
        return newList;
    }

    private List<Index> replaceContainingWithConstrained(Index containingIndex, Index constrainedContainingIndex, List<Index> indexes) {
        List<Index> newList = new ArrayList<>(indexes.size() - 1);
        for (Index index : indexes) {
            if (index.equals(containingIndex)) {
                newList.add(constrainedContainingIndex);
            } else {
                newList.add(index);
            }
        }
        return newList;
    }

    private static List<Pair<Index, Index>> getRemainingCcPairs(List<Pair<Index, Index>> containedContainingIndexPairs,
                                                                int indexOfRemoved,
                                                                Index constrainedContainingIndex) {
        Index removed = containedContainingIndexPairs.get(indexOfRemoved).getLeft();
        Index oldContaining = containedContainingIndexPairs.get(indexOfRemoved).getRight();

        List<Pair<Index, Index>> remainingCcPairs = new ArrayList<>();
        for (Pair<Index, Index> pair : containedContainingIndexPairs) {
            Index currContaining = pair.getRight();
            Index currContained = pair.getLeft();

            //remove all pairs that has the removed index in contained or containing
            if (removed.equals(currContaining) || removed.equals(currContained)) {
                continue;
            }

            if (currContaining == oldContaining) {
                //as the containing has additional constraints now, we need to recheck whether it still contains the
                // contained
                if (isContained(currContained, constrainedContainingIndex)) {
                    //replace old containing with the new constrained containing
                    pair = Pair.of(currContained, constrainedContainingIndex);
                } else {
                    continue;
                }
            }
            remainingCcPairs.add(pair);
        }
        return remainingCcPairs;
    }

    private static List<Pair<Index, Index>> getRemainingCcPairsForSkippingOptimization(List<Pair<Index, Index>> containedContainingIndexPairs,
                                                                int indexOfRemoved) {
        List<Pair<Index, Index>> remainingCcPairs = new ArrayList<>();
        for (int i = 0; i < containedContainingIndexPairs.size(); i++) {
            if (i == indexOfRemoved) {
                continue;
            }
            remainingCcPairs.add(containedContainingIndexPairs.get(i));
        }
        return remainingCcPairs;
    }

    private List<Index> choseBestIndexes(List<List<Index>> newIndexListCandidates) {
        return indexListSelectionStrategy.choseBestIndexes(newIndexListCandidates);
    }

    private static List<Pair<Index, Index>> calculateContainedContainingIndexPairs(List<Index> indexes) {
        List<Pair<Index, Index>> pairs = new ArrayList<>();
        // look for contained indexes only after the analyzed containing index (they are sorted by length desc)
        for (int i = 0; i < indexes.size(); i++) {
            Index containingIndex = indexes.get(i);
            for (int j = i + 1; j < indexes.size(); j++) {
                Index containedIndex = indexes.get(j);
                if (containingIndex.getLength() > containedIndex.getLength()
                                                    && isContained(containedIndex, containingIndex)) {
                    pairs.add(Pair.of(containedIndex, containingIndex));
                }
            }
        }
        return pairs;
    }

    private static boolean isContained(Index containedIndex, Index containingIndex) {
        int currFieldSetIndexInContainingIndex = -1;
        int fieldsNumLeftToCheckInCurrSet = 0;
        IndexFieldSet currFieldSetInContainingIndex = null;

        for (IndexFieldSet fieldSet : containedIndex.getFieldSets()) {
            for (IndexField field : fieldSet.getFields()) {
                if (fieldsNumLeftToCheckInCurrSet == 0) {
                    currFieldSetIndexInContainingIndex++;
                    if (currFieldSetIndexInContainingIndex >= containingIndex.getFieldSets().size()) {
                        throw new IllegalStateException("The contained index should be shorter than the containing index.");
                    }
                    currFieldSetInContainingIndex = containingIndex.getFieldSets().get(currFieldSetIndexInContainingIndex);
                    fieldsNumLeftToCheckInCurrSet = currFieldSetInContainingIndex.getLength();
                }
                if (currFieldSetInContainingIndex.contains(field)) {
                    //OK, just continue
                    fieldsNumLeftToCheckInCurrSet--;
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    private static Index addConstraintsToContainingIndex(Index containedIndex, Index containingIndex) {
        //add constraints on the prefix -> the prefix needs to contain fields of the remove index
        //which means: move the elements from the field sets of the removed index to the front of the constrained index
        //and remove them form the remaining groups in the constrained index

        Index constrainedIndex = new Index();
        for (IndexFieldSet fieldSet : containedIndex.getFieldSets()) {
            constrainedIndex.addFieldSet(fieldSet);
        }
        List<IndexField> fieldsToRemoveInTail = containedIndex.getFieldSets().stream()
            .flatMap(fs -> fs.getFields().stream())
            .collect(Collectors.toCollection(LinkedList::new));
        for (IndexFieldSet fieldSet : containingIndex.getFieldSets()) {
            if (!fieldsToRemoveInTail.isEmpty()) {
                IndexField toRemove = fieldsToRemoveInTail.get(0);
                if (fieldSet.contains(toRemove)) {
                    //make a copy:
                    fieldSet = new IndexFieldSet(fieldSet.getFields());
                }
                while (!fieldsToRemoveInTail.isEmpty() && fieldSet.contains(toRemove)) {
                    fieldSet.remove(toRemove);
                    fieldsToRemoveInTail.remove(0);
                    if (! fieldsToRemoveInTail.isEmpty()) {
                        toRemove = fieldsToRemoveInTail.get(0);
                    }
                }
            }
            if (fieldSet.getLength() > 0) {
                constrainedIndex.addFieldSet(fieldSet);
            }
        }
        return constrainedIndex;
    }

    void setMemoize(boolean memoize) {
        this.memoize = memoize;
    }

}
