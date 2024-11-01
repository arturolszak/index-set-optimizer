package indexoptimization;

import java.util.ArrayList;
import java.util.List;

public class MinSumOfSquaresIndexListSelectionStrategy implements IndexListSelectionStrategy {
    @Override
    public List<List<Index>> choseBestIndexSets(List<List<Index>> indexCandidates) {
        //chose the best set (smallest number of indexes)
        int minSumOfSquares = -1;
        List<List<Index>> bestIndexSetList = null;
        for (List<Index> indexes : indexCandidates) {
            int sumOfSquares = indexes.stream().mapToInt(Index::getLength).map(len -> len * len).sum();
            if (minSumOfSquares == -1 || sumOfSquares < minSumOfSquares) {
                bestIndexSetList = new ArrayList<>();
                bestIndexSetList.add(indexes);
                minSumOfSquares = sumOfSquares;
            }
            else if (sumOfSquares == minSumOfSquares) {
                bestIndexSetList.add(indexes);
            }
        }
        return bestIndexSetList;
    }
}