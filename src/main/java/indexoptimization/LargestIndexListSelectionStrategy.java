package indexoptimization;

import java.util.List;

public class LargestIndexListSelectionStrategy implements IndexListSelectionStrategy {
    @Override
    public List<Index> choseBestIndexes(List<List<Index>> indexCandidates) {
        //chose the best set (smallest number of indexes)
        int maxSize = -1;
        List<Index> bestIndexList = null;
        for (List<Index> indexes : indexCandidates) {
            if (indexes.size() > maxSize) {
                bestIndexList = indexes;
                maxSize = bestIndexList.size();
            }
        }
        return bestIndexList;
    }
}
