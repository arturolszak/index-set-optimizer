package indexoptimization;

import java.util.List;

public interface IndexListSelectionStrategy {

    List<List<Index>> choseBestIndexSets(List<List<Index>> indexColCandidates);

    default List<Index> choseBestIndexSet(List<List<Index>> indexColCandidates) {
        List<List<Index>> bestIndexSets = choseBestIndexSets(indexColCandidates);
        return bestIndexSets.isEmpty() ? null : bestIndexSets.get(0);
    }


}
