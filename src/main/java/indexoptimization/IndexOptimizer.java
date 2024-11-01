package indexoptimization;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * IndexOptimizer generates an optimal subset of indexes, based on the set of indexes provided as input.
 * The optimizer seeks for a possibility of covering one index with another index in the input by reordering the fields
 * in the index. Removing an index, however imposes some extra constraints on the order of the covering index. It can
 * happen that these additional constraints might make it not possible to remove another index, which could be covered
 * by the constrained index if the constraints were different. The objective of the tool is to find an optimal set of
 * indexes with the necessary field order constraints. The index set selection criteria (defining what the "optimal"
 * index set is) can be passed to the IndexOptimizer as a strategy object. The default strategy selects the index set
 * containing the smallest number of indexes.
 *
 * An index used by the optimizer is represented by a sequence of fields sets, for example the following index:
 *
 *     {{r,g}{d}{e}}
 *
 * is composed of three field sets: `{r,g}`, `{d}`, and `{e}`.
 * The order of the groups must be preserved, but the elements might be permuted within individual groups.
 *
 * For example if an index is used only for equality search, its fields can be reordered, and the index is semantically
 * the same. However, if the index is intended to be used for sorting, the order of the fields is important and should
 * be preserved. The ESR (Equality, Sort, Range) rule suggests that the equality fields (an exact match on s single
 * value) should be placed in the index before the sorting fields, and fields used as range filters in most cases
 * should be placed after the sorting fields. By providing constrained indexes in the input, the optimizer can be
 * forced to respect the constraints on the order of index fields that are known upfront.
 *
 * For example, in the index below:
 *
 *     {{e1,e2,e3}{s1}{s2}{r1}{r2}}
 *
 * only the fields `e1`, `e2` and `e3` can be freely reordered.
 *
 * ### Algorithm
 *
 * Internally, the optimizer does a recursive state space search with memoization. Initially, a set of pairs of indexes
 * and indexes that are contained within them is generated. Then on each recursion level, the algorithm branches,
 * analyzing a single pair in each branch:
 * - trying to remove the index contained in the containing index
 * - applying additional constraints on the containing index field order caused by removing the contained index
 * - rechecking which of the remaining potential pairs are still valid (contained-containing relation might not be
 *   valid after adding the constraints, and the pairs having the removed index as the contained or the containing
 *   index
 *   should be removed)
 * - solving the sub-problem for the new set of indexes and the remaining contained-containing pairs
 * After collecting the results from the recursive calls, the best set is selected according to the configured strategy.
 *
 * ### Example
 *
 * For the given index set:
 *
 *     {{a,d,f,g,j,n,r,t,z}}
 *     {{d,g,r}}
 *     {{a,z}}
 *     {{b,r}}
 *     {{g}}
 *
 * the tool generates the following optimal index set:
 *
 *     {{g}{r,d}{a,f,t,j,n,z}}
 *     {{a,z}
 *     {{b,r}}
 */
public class IndexOptimizer {

    // compile-time config:
    private static final boolean removeDuplicateFields = false;
    public static final int FAST_OPTIMIZER_MAX_NUM_PATHS_PER_STEP = 5;

    // runtime config:
    private boolean maskKeyNames;
    private boolean memoize;
    private int numThreads;
    private IndexListSelectionStrategy indexListSelectionStrategy;
    private boolean shouldConsiderSkippedOptimizations;
    private int maxNumPathsPerStep;

    private final Map<String, List<Index>> optimizedIndexesMemoizer;

    private IndexOptimizer() {
        this.maskKeyNames = true;
        this.memoize = true;
        this.numThreads = 1;
        this.indexListSelectionStrategy = new ChainingIndexListSelectionStrategy(
                new SmallestIndexListSelectionStrategy(),
                new MinSumOfSquaresIndexListSelectionStrategy()
        );
        this.shouldConsiderSkippedOptimizations = false;
        this.maxNumPathsPerStep = -1;

        this.optimizedIndexesMemoizer = initializeMemoizer();
    }

    public static IndexOptimizer createDefaultSingleThreadedOptimizer() {
        return new IndexOptimizer();
    }

    public static IndexOptimizer createFastSingleThreadedOptimizer() {
        IndexOptimizer indexOptimizer = new IndexOptimizer();
        indexOptimizer.setMaxNumPathsPerStep(FAST_OPTIMIZER_MAX_NUM_PATHS_PER_STEP);
        return indexOptimizer;
    }

    public static IndexOptimizer createDefaultMultiThreadedOptimizer() {
        IndexOptimizer indexOptimizer = new IndexOptimizer();
        indexOptimizer.setNumThreads(Runtime.getRuntime().availableProcessors());
        return indexOptimizer;
    }

    public static IndexOptimizer createFastMultiThreadedOptimizer() {
        IndexOptimizer indexOptimizer = new IndexOptimizer();
        indexOptimizer.setMaxNumPathsPerStep(FAST_OPTIMIZER_MAX_NUM_PATHS_PER_STEP);
        indexOptimizer.setNumThreads(Runtime.getRuntime().availableProcessors());
        return indexOptimizer;
    }

    private Map<String, List<Index>> initializeMemoizer() {
        return (numThreads == 1 ? new HashMap<>() : new ConcurrentHashMap<>());
    }

    public List<Index> optimizeIndexes(List<Index> indexes) {
        indexes = sanitizeIndexes(indexes);
        Map<String, String> mapping = new HashMap<>();
        if (maskKeyNames) {
            mapping = calculateFieldMapping(indexes);
            indexes = mapIndexes(indexes, mapping);
        }
        List<Pair<Index, Index>> containedContainingIndexPairs = calculateContainedContainingIndexPairs(indexes);
        if (!containedContainingIndexPairs.isEmpty()) {
            List<Index> optimized = optimizeIndexes(indexes, containedContainingIndexPairs);
            optimized = mapIndexes(optimized, reverseMapping(mapping));
            return optimized;
        } else {
            indexes = mapIndexes(indexes, reverseMapping(mapping));
            return indexes;
        }
    }

    private List<Index> optimizeIndexes(List<Index> indexes, List<Pair<Index, Index>> containedContainingIndexPairs) {
        List<Index> optimized;
        if (numThreads > 1) {
            List<Future<List<Index>>> futures = new ArrayList<>();
            ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
            List<Pair<Index, Index>> containedContainingIndexPairsSync =
                    Collections.synchronizedList(containedContainingIndexPairs);

            sortContainedContainingIndexPair(containedContainingIndexPairs, 0, containedContainingIndexPairs.size());
            int to = maxNumPathsPerStep >= 0
                    ? Math.min(containedContainingIndexPairs.size(), maxNumPathsPerStep)
                    : containedContainingIndexPairs.size();

            for (int i = 0; i < to; i++) {
                int ccRangeLow = i;
                int ccRangeHigh = i + 1;
                Future<List<Index>> future = executorService.submit(
                        () -> optimizeIndexesRecursive(indexes,
                                                       containedContainingIndexPairsSync,
                                                       ccRangeLow,
                                                       ccRangeHigh,
                                                       0)
                );
                futures.add(future);
            }
            List<List<Index>> optimizedSublists = new ArrayList<>();
            for (Future<List<Index>> future : futures) {
                try {
                    optimizedSublists.add(future.get());
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            optimized = choseBestIndexes(optimizedSublists);
        } else {
            optimized = optimizeIndexesRecursive(indexes,
                                                 containedContainingIndexPairs,
                                                 0,
                                                 containedContainingIndexPairs.size(),
                                                 0);
        }
        return optimized;
    }

    @NotNull
    static Map<String, String> calculateFieldMapping(List<Index> indexes) {
        Map<String, String> mapping = new HashMap<>();
        for (Index index : indexes) {
            for (IndexFieldSet fieldSet : index.fieldSets) {
                for (IndexField field : fieldSet.getFields()) {
                    if (!mapping.containsKey(field.getName())) {
                        mapping.put(field.getName(), Integer.toString(mapping.size()));
                    }
                }
            }
        }
        return mapping;
    }

    @NotNull
    static List<Index> mapIndexes(List<Index> indexes, Map<String, String> mapping) {
        if (mapping.isEmpty()) {
            return indexes;
        }
        List<Index> newIndexes = new ArrayList<>();
        for (Index index : indexes) {
            List<IndexFieldSet> newFieldSets = new ArrayList<>();
            for (IndexFieldSet fieldSet : index.fieldSets) {
                Set<IndexField> indexFields = fieldSet.getFields().stream()
                        .map(f -> new IndexField(mapping.getOrDefault(f.getName(), f.getName())))
                        .collect(Collectors.toSet());
                IndexFieldSet newIndexFieldSet = new IndexFieldSet(indexFields);
                newFieldSets.add(newIndexFieldSet);
            }
            Index newIndex = new Index(newFieldSets);
            newIndexes.add(newIndex);
        }
        return newIndexes;
    }

    private Map<String, String> reverseMapping(Map<String, String> mapping) {
        Map<String, String> reverseMapping = new HashMap<>();
        for (Map.Entry<String, String> entry : mapping.entrySet()) {
            reverseMapping.put(entry.getValue(), entry.getKey());
        }
        return reverseMapping;
    }

    static List<Index> sanitizeIndexes(List<Index> indexes) {
        return indexes.stream()
                .map(index -> removeDuplicateFields ? removeDuplicateFields(index) : index)
                .map(IndexOptimizer::removeEmptyFieldSets)
                .filter(index -> !index.fieldSets.isEmpty())
                .collect(Collectors.toList());

    }

    private static Index removeDuplicateFields(Index index) {
        Index newIndex = new Index(index.fieldSets);
        Set<IndexField> fieldsSeenSoFar = new HashSet<>();
        for (IndexFieldSet fieldSet : newIndex.fieldSets) {
            Set<IndexField> dupsToRemove = new HashSet<>();
            for (IndexField field : fieldSet.getFields()) {
                if (fieldsSeenSoFar.contains(field)) {
                    dupsToRemove.add(field);
                } else {
                    fieldsSeenSoFar.add(field);
                }
            }
            for (IndexField dup : dupsToRemove) {
                fieldSet.remove(dup);
            }
        }
        return newIndex;
    }

    private static Index removeEmptyFieldSets(Index index) {
        List<IndexFieldSet> newFieldSets = index.fieldSets.stream()
                .filter(fieldSet -> fieldSet.getLength() > 0)
                .collect(Collectors.toCollection(ArrayList::new));
        return new Index(newFieldSets);
    }

    private List<Index> optimizeIndexesRecursive(List<Index> indexes,
                                                 List<Pair<Index, Index>> containedContainingIndexPairs,
                                                 int from,
                                                 int to,
                                                 int level) {
        // sorted index list and sorted fields within field sets, so the key string is always the same for memoization
        String s = indexes.stream().map(Index::toStringSorted).sorted().collect(Collectors.joining(", "));
        if (memoize) {
            List<Index> optimizedIndexes = optimizedIndexesMemoizer.get(s);
            if (optimizedIndexes != null) {
                return optimizedIndexes;
            }
        }

        List<List<Index>> newIndexListCandidates = doOptimizeIndexesRecursive(indexes,
                                                                              containedContainingIndexPairs,
                                                                              from,
                                                                              to,
                                                                              level);

        //chose the best according to the chosen strategy, and return it
        List<Index> optimizedIndexes = choseBestIndexes(newIndexListCandidates);

        if (memoize) {
            optimizedIndexesMemoizer.put(s, optimizedIndexes);
        }
        return optimizedIndexes;
    }

    private List<List<Index>> doOptimizeIndexesRecursive(List<Index> indexes,
                                                         List<Pair<Index, Index>> containedContainingIndexPairs,
                                                         int from,
                                                         int to,
                                                         int level) {
        List<List<Index>> newIndexListCandidates = new ArrayList<>();

        sortContainedContainingIndexPair(containedContainingIndexPairs, from, to);
        to = maxNumPathsPerStep >= 0 ? Math.min(to, from + maxNumPathsPerStep) : to;

        //remove one by one from contained indexes and proceed recursively with remaining contained indexes
        for (int i = from; i < to; i++) {
            Pair<Index, Index> cc = containedContainingIndexPairs.get(i);
            Index containedIndex = cc.getLeft();
            Index containingIndex = cc.getRight();

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
                List<Index> indexesAfterRecursiveRemoval = optimizeIndexesRecursive(indexesAfterRemovingOneAndConstraining,
                                                                                    remainingCcPairs,
                                                                                    0,
                                                                                    remainingCcPairs.size(),
                                                                                    level + 1);
                newIndexListCandidates.add(indexesAfterRecursiveRemoval);
            } else {
                newIndexListCandidates.add(indexesAfterRemovingOneAndConstraining);
            }

            if (shouldConsiderSkippedOptimizations) {
                List<Pair<Index, Index>> remainingCcPairsAfterSkipping = getRemainingCcPairsForSkippingOptimization(containedContainingIndexPairs, i);
                if (! remainingCcPairsAfterSkipping.isEmpty()) {
                    List<Index> indexesAfterRecursiveRemoval = optimizeIndexesRecursive(indexes,
                                                                                        remainingCcPairsAfterSkipping,

                                                                                        0,
                                                                                        remainingCcPairsAfterSkipping.size(),
                                                                                        level + 1);
                    newIndexListCandidates.add(indexesAfterRecursiveRemoval);
                } else {
                    newIndexListCandidates.add(indexes);
                }
            }

        }

        return newIndexListCandidates;
    }

    private void sortContainedContainingIndexPair(List<Pair<Index, Index>> containedContainingIndexPairs,
                                                  int from,
                                                  int to) {
        HashMap<String, Integer> containedCounts = new HashMap<>();
        for (Pair<Index, Index> containedContainingIndexPair : containedContainingIndexPairs) {
            String containing = containedContainingIndexPair.getRight().toStringSorted();
            Integer containedCount = containedCounts.getOrDefault(containing, 0);
            containedCounts.put(containing, containedCount + 1);
        }

        containedContainingIndexPairs.subList(from, to).sort(Comparator
                                                                     .comparing(p -> containedCounts.get(((Pair<Index, Index>) p).getRight().toStringSorted()))
                                                                     .thenComparing(p -> -((Pair<Index, Index>) p).getRight().getLength())
                                                                     .thenComparing(p -> ((Pair<Index, Index>) p).getLeft().getLength())
        );
    }

    private List<Index> removeIndex(Index contained, List<Index> indexes) {
        List<Index> newList = new ArrayList<>(indexes.size() - 1);
        for (Index index : indexes) {
            if (!index.equals(contained)) {
                newList.add(index);

            } // else skip (remove)
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
            Index currContianing = pair.getRight();
            Index currContained = pair.getLeft();

            //remove all pairs that has the removed index in contained or containing
            if (removed.equals(currContianing) || removed.equals(currContained)) {
                continue;
            }

            if (currContianing == oldContaining) {
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
        return indexListSelectionStrategy.choseBestIndexSet(newIndexListCandidates);
    }

    private static List<Pair<Index, Index>> calculateContainedContainingIndexPairs(List<Index> indexes) {
        List<Pair<Index, Index>> pairs = new ArrayList<>();
        for (int i = 0; i < indexes.size(); i++) {
            Index containingIndex = indexes.get(i);
            for (int j = 0; j < indexes.size(); j++) {
                Index containedIndex = indexes.get(j);
                if (i != j
                        && containingIndex.getLength() >= containedIndex.getLength()
                        && isContained(containedIndex, containingIndex)) {
                    pairs.add(Pair.of(containedIndex, containingIndex));
                }
            }
        }
        return pairs;
    }

    static boolean isContained(Index containedIndex, Index containingIndex) {
        int i; // i -> curr. field set in contained index
        int j = 0; // j -> curr. field set in containing index
        int m = 0; // matched in current field set
        for (i = 0; i < containedIndex.getFieldSets().size(); i++) {
            if (j == containingIndex.getFieldSets().size()) {
                // all containing containing index field sets were used up, and the containing index is not covered
                return false;
            }
            for (IndexField fieldInContained: containedIndex.getFieldSets().get(i).getFields()) {
                if (containingIndex.getFieldSets().get(j).contains(fieldInContained)) {
                    m++;
                } else {
                    return false;
                }
            }
            if (m == containingIndex.getFieldSets().get(j).getLength()) {
                // all fields in containing field set were used up, move to the next group
                // m cannot be greater than length, otherwise, false would be returned above
                m = 0; // reset the counter
                j++; // move to the next field set in containing index
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


    // setters to configure the behavior of the index optimizer, used for testing

    @VisibleForTesting void setMaskKeyNames(boolean maskKeyNames) {
        this.maskKeyNames = maskKeyNames;
    }

    @VisibleForTesting void setMemoize(boolean memoize) {
        this.memoize = memoize;
    }

    @VisibleForTesting void setNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }

    @VisibleForTesting void setIndexListSelectionStrategy(IndexListSelectionStrategy indexListSelectionStrategy) {
        this.indexListSelectionStrategy = indexListSelectionStrategy;
    }

    @VisibleForTesting void setShouldConsiderSkippedOptimizations(boolean shouldConsiderSkippedOptimizations) {
        this.shouldConsiderSkippedOptimizations = shouldConsiderSkippedOptimizations;
    }

    @VisibleForTesting void setMaxNumPathsPerStep(int maxNumPathsPerStep) {
        this.maxNumPathsPerStep = maxNumPathsPerStep;
    }

}
