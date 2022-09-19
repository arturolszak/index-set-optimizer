package indexoptimization;

import java.util.List;

public interface IndexListSelectionStrategy {
    List<Index> choseBestIndexes(List<List<Index>> indexColCandidates);
}
