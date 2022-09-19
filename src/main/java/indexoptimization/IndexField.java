package indexoptimization;

import java.util.Comparator;
import java.util.Objects;

public class IndexField implements Comparable<IndexField> {
    private final String name;

    private static final Comparator<IndexField> COMP = Comparator.comparing(f -> f.name);

    public String getName() {
        return name;
    }

    public IndexField(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexField that = (IndexField) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public int compareTo(IndexField o) {
        return COMP.compare(this, o);
    }

    public String toString() {
        return name;
    }

}
