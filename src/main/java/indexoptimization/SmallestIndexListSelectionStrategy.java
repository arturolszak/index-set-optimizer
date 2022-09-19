package indexoptimization;

import java.util.List;

public class SmallestIndexListSelectionStrategy implements IndexListSelectionStrategy {
    @Override
    public List<Index> choseBestIndexes(List<List<Index>> indexCandidates) {
        //chose the best set (smallest number of indexes)
        int minSize = -1;
        List<Index> bestIndexList = null;
        for (List<Index> indexes : indexCandidates) {
            if (minSize == -1 || indexes.size() < minSize) {
                bestIndexList = indexes;
                minSize = bestIndexList.size();
            }
        }
        return bestIndexList;
    }
}
