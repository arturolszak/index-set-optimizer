package indexoptimization;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IndexTest {

    @Test
    public void testParseAndToString() {
        String indexStr = "{{a,d,f,g,j,n,r,t,z}}";
        Index index = Index.parseIndex(indexStr);
        System.out.println(index);
        assertEquals("{{a,d,f,g,j,n,r,t,z}}", index.toString());
    }

    @Test
    public void testParseAndToStringWithGroups() {
        String indexStr = "{{a,d}{f}{g,j}{n,r,t,z}}";
        Index index = Index.parseIndex(indexStr);
        System.out.println(index);
        assertEquals("{{a,d}{f}{g,j}{n,r,t,z}}", index.toString());
    }

}
