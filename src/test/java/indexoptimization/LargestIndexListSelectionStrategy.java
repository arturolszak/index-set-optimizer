package indexoptimization;

import java.util.ArrayList;
import java.util.List;

public class LargestIndexListSelectionStrategy implements IndexListSelectionStrategy {
    @Override
    public List<List<Index>> choseBestIndexSets(List<List<Index>> indexCandidates) {
        //chose the set with the largest number of indexes
        int maxSize = -1;
        List<List<Index>> bestIndexSetList = null;
        for (List<Index> indexes : indexCandidates) {
            if (indexes.size() > maxSize) {
                bestIndexSetList = new ArrayList<>();
                bestIndexSetList.add(indexes);
                maxSize = indexes.size();
            } else if (indexes.size() == maxSize) {
                bestIndexSetList.add(indexes);
            }
        }
        return bestIndexSetList;
    }
}
