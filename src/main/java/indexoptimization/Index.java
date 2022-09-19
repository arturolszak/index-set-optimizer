package indexoptimization;

import java.util.ArrayList;
import java.util.List;

public class Index {

    List<IndexFieldSet> fieldSets;

    public Index() {
        fieldSets = new ArrayList<>();
    }

    public Index(List<IndexField> fields) {
        this();
        IndexFieldSet fieldSet = new IndexFieldSet();
        for (IndexField field : fields) {
            fieldSet.add(field);
        }
        fieldSets.add(fieldSet);
    }

    public List<IndexFieldSet> getFieldSets() {
        return new ArrayList<>(fieldSets);
    }

    public void addFieldSet(IndexFieldSet indexFieldSet) {
        fieldSets.add(indexFieldSet);
    }

    public int getLength() {
        return fieldSets.stream()
            .mapToInt(IndexFieldSet::getLength)
            .sum();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (IndexFieldSet fieldSet : fieldSets) {
            sb.append(fieldSet.toString());
        }
        sb.append("}");
        return sb.toString();
    }

    public String toStringSorted() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (IndexFieldSet fieldSet : fieldSets) {
            sb.append(fieldSet.toStringSorted());
        }
        sb.append("}");
        return sb.toString();
    }

    public static Index parseIndex(String s) {
        return IndexParser.parseIndex(s);
    }


}
