package indexoptimization;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class IndexFieldSet {
    private final Set<IndexField> fields;

    public IndexFieldSet() {
        fields = new HashSet<>();
    }

    public IndexFieldSet(Set<IndexField> fields) {
        this();
        this.fields.addAll(fields);
    }

    public Set<IndexField> getFields() {
        return new LinkedHashSet<>(fields);
    }

    void add(IndexField field) {
        fields.add(field);
    }

    public boolean contains(IndexField field) {
        return fields.contains(field);
    }

    void remove(IndexField field) {
        fields.remove(field);
    }

    public int getLength() {
        return fields.size();
    }

    public String toString() {
        String sb = "{" +
                fields.stream().map(IndexField::toString).collect(Collectors.joining(",")) +
                "}";
        return sb;
    }

    public String toStringSorted() {
        String sb = "{" +
                fields.stream().map(IndexField::toString).sorted().collect(Collectors.joining(",")) +
                "}";
        return sb;
    }
}
