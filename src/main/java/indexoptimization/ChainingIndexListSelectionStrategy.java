package indexoptimization;

import java.util.ArrayList;
import java.util.List;

public class ChainingIndexListSelectionStrategy implements IndexListSelectionStrategy {

    private final List<IndexListSelectionStrategy> strategies;

    public ChainingIndexListSelectionStrategy(IndexListSelectionStrategy... strategies) {
        this.strategies = List.of(strategies);
    }

    @Override
    public List<List<Index>> choseBestIndexSets(List<List<Index>> indexCandidates) {
        for (IndexListSelectionStrategy strategy : strategies) {
            indexCandidates = strategy.choseBestIndexSets(indexCandidates);
            if (indexCandidates.size() <= 1) {
                break;
            }
        }
        return indexCandidates;
    }
}