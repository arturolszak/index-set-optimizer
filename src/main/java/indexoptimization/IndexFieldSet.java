package indexoptimization;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class IndexFieldSet {
    private final List<IndexField> fields;

    public IndexFieldSet() {
        fields = new ArrayList<>();
    }

    public IndexFieldSet(List<IndexField> fields) {
        this();
        this.fields.addAll(fields);
    }

    public List<IndexField> getFields() {
        return new ArrayList<>(fields);
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
        return "{" +
                fields.stream().map(IndexField::toString).collect(Collectors.joining(",")) +
                "}";
    }

    public String toStringSorted() {
        return "{" +
                fields.stream().map(IndexField::toString).sorted().collect(Collectors.joining(",")) +
                "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof IndexFieldSet))
            return false;
        IndexFieldSet that = (IndexFieldSet) o;
        return Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fields);
    }

    public IndexFieldSet copy(){
        return new IndexFieldSet(fields);
    }
}
