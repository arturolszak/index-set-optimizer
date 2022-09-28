package indexoptimization;

public class IndexParser {
    public static Index parseIndex(String s) {
        Index index = new Index();
        int i = 0;
        i += accept(s, i, '{');
        while (i < s.length() && s.charAt(i) == '{') {
            IndexFieldSet fieldSet = new IndexFieldSet();
            index.fieldSets.add(fieldSet);
            i += accept(s, i, '{');
            String fieldSetSubstring = s.substring(i, s.indexOf('}', i));
            String[] fields = fieldSetSubstring.split(",");
            for (String field : fields) {
                if (!field.isBlank()) {
                    fieldSet.add(new IndexField(field));
                }
            }
            i += fieldSetSubstring.length();
            i += accept(s, i, '}');
        }
        accept(s, i, '}');
        return index;
    }

    private static int accept(String s, int i, char c) {
        if (s.charAt(i) == c) {
            return 1;
        }
        throw new IllegalArgumentException();
    }

}
