
### General information

The tool (`IndexOptimizer` class) generates an optimal subset of indexes, based on the set of indexes provided as an input.
The optimizer seeks for a possibility of covering one index with another index in the input by reordering its fields 
in  the index. Removing an index, however imposes some extra constraints on the order of the covering index. It can happen that these additional constraints might make it not possible to remove another index, which could be covered by the constrained index with if the constraints were different. The objective of the tool is to find an optimal set of indexes with the necessary field order constraints. The index set selection criteria (defining what the "optimal" index set is) can be injected to the IndexOptimizer as a strategy. The default strategy selects the index set containing the smaller number of indexes.

An index used by the optimizer is represented by a number of fields sets, for example the following index:

    {{r,g}{d}{e}}

is composed of three field sets: `{r,g}`, `{d}`, and `{e}`.
The order of the groups must be preserved, but the elements might be permuted within individual groups.

For example if an index is used only for equality search, its fields can be reordered, and the index is semantically 
the same. However, if the index is intended to be used for sorting, the order of the fields is important and should 
be preserved. The ESR (Equality, Sort, Range) rule suggests that the equality fields (an exact match on s single 
value) should be placed in the index before the sorting fields, and fields used as range filters in most cases 
should be placed after the sorting fields. By providing constrained indexes in the input, the optimizer can be 
forced to respect the constraints on the order of index fields that are known upfront.

For example in the index below:

    {{e1,e2,e3}{s1}{s2}{r1}{r2}}

only the fields e1, e2 and e3 can be freely reordered.

### Algorithm

Internally the optimizer does a recursive state space search with memoization. Initially, a set of pairs of indexes and indexes that are contained within them is generated. Then on each recursion level, the algorithm branches by analyzing a single pair:
- trying to remove the index contained in the second index
- applying additional constraints on the containing index caused by removing the contained index
- rechecking which of the remaining potential pairs are still valid (contained-containing relation)
- solving the sub-problem for the new set of indexes and remaining optimization pairs
After collecting the results from all recursive calls, the best is selected according to the configured strategy.


### Example

For the given index set:

    {{a,d,f,g,j,n,r,t,z}}
    {{d,g,r}}
    {{a,z}}
    {{b,r}}
    {{g}}

the tool generates the following optimal index set:

    {{g}{r,d}{a,f,t,j,n,z}}
    {{a,z}
    {{b,r}}
