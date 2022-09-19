package indexoptimization;

import java.util.HashSet;
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


    public boolean isInGroup(IndexField field) {
        return fields.contains(field);
    }

    public Set<IndexField> getFields() {
        return new HashSet<>(fields);
    }

    public void add(IndexField field) {
        fields.add(field);
    }

    public boolean contains(IndexField field) {
        return fields.contains(field);
    }

    public void remove(IndexField field) {
        fields.remove(field);
    }

    public int getLength() {
        return fields.size();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append(
            fields.stream().map(IndexField::toString).collect(Collectors.joining(","))
        );
        sb.append("}");
        return sb.toString();
    }

    public String toStringSorted() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append(
            fields.stream().map(IndexField::toString).sorted().collect(Collectors.joining(","))
        );
        sb.append("}");
        return sb.toString();
    }
}
