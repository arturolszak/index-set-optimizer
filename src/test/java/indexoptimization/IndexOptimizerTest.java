package indexoptimization;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

class IndexOptimizerTest {

    public static final String[] LONG_INPUT_INDEX_SET_STRINGS = {
            "{{0}}",
            "{{1,2,3,4}}",
            "{{2,3,4,5}{6}}",
            "{{1,3,7,8,9}}",
            "{{2,3}}",
            "{{10,2,3,4}}",
            "{{2,3,4}{11}}",
            "{{12,2,3,4}}",
            "{{2,3,4}}",
            "{{1,12,13,14,15,2,3,7,8}}",
            "{{2,3,4,7}}",
            "{{16,2}}",
            "{{17,2,3}}",
            "{{17,18,2,3}}",
            "{{1,2,3,4}}",
            "{{15,16,17,18,19,2,20,21,22,3,4}{23,24}}",
            "{{25}}",
            "{{16,2}}",
            "{{2,26,3}}",
            "{{27,3}}",
            "{{1,2,4,5,9}}",
            "{{2,22}}",
            "{{15,17,19,2,20,28}}",
            "{{12,15,2,28}}",
            "{{3}}",
            "{{21}}",
            "{{16,17,18,19,2,20,21}}",
            "{{2,21}}",
            "{{1,2,3}}"
    };
    public static final String[] LONG_INPUT_INPUT_SET_LONG_STRINGS = {
            "{{}{SimpleField[attribute=Territory.Status; isRangeQuery=true]}}",
            "{{SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.DistrictUuid; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.CloseDate; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.StaffUuid; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.CloseDate; isRangeQuery=false]}{SimpleField[attribute=Territory.Uuid; isRangeQuery=true]}}",
            "{{SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.CompanyUuid; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.DistrictUuid; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.RegionUuid; isRangeQuery=false],SimpleField[attribute=Territory.Uuid; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.CompanyStatus; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.CloseDate; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.CloseDate; isRangeQuery=false]}{SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.CompanyUuid; isRangeQuery=true]}}",
            "{{SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.RegionNumber; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.CloseDate; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.CloseDate; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.DistrictNumber; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.CompanyNumber; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.RegionNumber; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.DistrictUuid; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.CompanyUuid; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.RegionUuid; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingInformation.ReportingEndDate; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.CompanyUuid; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.CloseDate; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryInformation.TerritoryNumber; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingDistrictRef; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingCompanyRef; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingDistrictRef; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.DistrictUuid; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.CloseDate; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingCompanyRef; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingRegionRef; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingDistrictRef; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingStaffRef; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryID; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.TerritoryNumber; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingInformation.ReportingEndDate; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.FormattedTerritoryNumber; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.CloseDate; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false]}{SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingInformation.ReportingStartDate; isRangeQuery=true],SimpleField[attribute=Territory.TerritoryInformation.OpenDate; isRangeQuery=true]}}",
            "{{}{SortField[order=DESC; attribute=Territory.TerritoryID]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.TerritoryNumber; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryInformation.TerritoryName; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.Status; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.TerritoryNumberGeneration.UnblockingLogic.UnBlockResponse; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.DistrictUuid; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.StaffUuid; isRangeQuery=false],SimpleField[attribute=Territory.Uuid; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.CloseDate; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryInformation.FormattedTerritoryNumber; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingRegionRef; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingDistrictRef; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingStaffRef; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingInformation.ReportingEndDate; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.HomeOffice; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.RegionNumber; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingInformation.ReportingEndDate; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.HomeOffice; isRangeQuery=false]}{}}",
            "{{}{}}",
            "{{SimpleField[attribute=Territory.Status; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryID; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingCompanyRef; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingRegionRef; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingDistrictRef; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.ReportingStaffRef; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryInformation.TerritoryNumber; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryID; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryID; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false]}{}}",
            "{{SimpleField[attribute=Territory.TerritoryService.ReportingTerritory.DistrictUuid; isRangeQuery=false],SimpleField[attribute=Territory.TerritoryService.TerritoryType; isRangeQuery=false],SimpleField[attribute=Territory.Status; isRangeQuery=false]}{}}"
    };

    @Test
    public void testParseAndToString() {
        String indexStr = "{{a,d,f,g,j,n,r,t,z}}";
        Index index = Index.parseIndex(indexStr);
        System.out.println(index);
    }

    @Test
    public void testParseAndToStringWithGroups() {
        String indexStr = "{{a,d}{f}{g,j}{n,r,t,z}}";
        Index index = Index.parseIndex(indexStr);
        System.out.println(index);
    }

    @Test
    public void testWithoutMemoization0() {
        // Arrange
        String[] inputIndexStrings = {
                "{{a,d,f,g,j,n,r,t,z}}",
                "{{d,g,r}}",
                "{{a,z}}",
                "{{b,r}}",
                "{{g}}"
        };

        List<Index> indexes = parseInputStrings(inputIndexStrings);

        // Act
        IndexOptimizer optimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        optimizer.setMemoize(false);
        List<Index> optimizedIndexes = optimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        Assertions.assertEquals(3, outputIndexStrings.size());
        Assertions.assertTrue(outputIndexStrings.contains("{{g}{d,r}{a,f,j,n,t,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{a,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{b,r}}"));
    }

    @Test
    public void testWithoutMemoization1() {
        // Arrange
        String[] inputIndexStrings = {
                "{{x,y,z}}",
                "{{z,y}}",
                "{{z,x}}",
                "{{x}}"
        };

        List<Index> indexes = parseInputStrings(inputIndexStrings);

        // Act
        IndexOptimizer indexOptimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        indexOptimizer.setMemoize(false);
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        Assertions.assertEquals(2, outputIndexStrings.size());
        Assertions.assertTrue(outputIndexStrings.contains("{{y,z}{x}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{x}{z}}"));
    }

    @Test
    @DisplayName("duplicates are removed")
    public void testWithoutMemoization2() {
        // Arrange
        List<Index> indexes = parseInputStrings(new String[]{
                "{{a,b}}",
                "{{a,b}}"
        });

        // Act
        IndexOptimizer optimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        optimizer.setMemoize(false);
        List<Index> optimizedIndexes = optimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        assertThat(outputIndexStrings, hasSize(1));
        assertThat(outputIndexStrings, contains("{{a,b}}"));
    }

    @Test
    @DisplayName("duplicates are removed (reversed order)")
    public void testWWithoutMemoization3() {
        // Arrange
        List<Index> indexes = parseInputStrings(new String[]{
                "{{a,b}}",
                "{{b,a}}"
        });

        // Act
        IndexOptimizer optimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        optimizer.setMemoize(false);
        List<Index> optimizedIndexes = optimizer.optimizeIndexes(indexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        assertThat(outputIndexStrings, hasSize(1));
        assertThat(outputIndexStrings, contains("{{a,b}}"));
    }

    @Test
    @DisplayName("order restriction is enforced")
    public void testWithoutMemoization4() {
        // Arrange
        List<Index> indexes = parseInputStrings(new String[]{
                "{{a,c}}",
                "{{a}{b}{c}}"
        });

        // Act
        IndexOptimizer optimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        optimizer.setMemoize(false);
        List<Index> optimizedIndexes = optimizer.optimizeIndexes(indexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        assertThat(outputIndexStrings, hasSize(2));
        assertThat(outputIndexStrings, containsInAnyOrder("{{a,c}}","{{a}{b}{c}}"));
    }

    @Test
    @DisplayName("optimization with empty field set")
    public void testWithoutMemoization5() {
        // Arrange
        List<Index> indexes = parseInputStrings(new String[]{
                "{{}{a,b}}",
                "{{b}{a}}"
        });

        // Act
        IndexOptimizer optimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        optimizer.setMemoize(false);
        List<Index> optimizedIndexes = optimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        assertThat(outputIndexStrings, hasSize(1));
        assertThat(outputIndexStrings, containsInAnyOrder("{{b}{a}}"));
    }

    @Test
    @DisplayName("optimization with empty field set 2")
    public void testWithoutMemoization6() {
        // Arrange
        List<Index> indexes = parseInputStrings(new String[]{
                "{{b}{a}{}}",
                "{{}{a,b}{}}"
        });

        // Act
        IndexOptimizer optimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        optimizer.setMemoize(false);
        List<Index> optimizedIndexes = optimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        assertThat(outputIndexStrings, hasSize(1));
        assertThat(outputIndexStrings, containsInAnyOrder("{{b}{a}}"));
    }

    @Test
    public void testWithMemoization0() {
        // Arrange
        String[] inputIndexStrings = {
                "{{a,d,f,g,j,n,r,t,z}}",
                "{{d,g,r}}",
                "{{a,z}}",
                "{{b,r}}",
                "{{g}}"
        };
        List<Index> indexes = parseInputStrings(inputIndexStrings);

        // Act
        IndexOptimizer indexOptimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        Assertions.assertEquals(3, outputIndexStrings.size());
        Assertions.assertTrue(outputIndexStrings.contains("{{g}{d,r}{a,f,j,n,t,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{a,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{b,r}}"));
    }

    @Test
    public void testWithMemoization1() {
        // Arrange
        String[] inputIndexStrings = {
                "{{x,y,z}}",
                "{{z,y}}",
                "{{z,x}}",
                "{{x}}"
        };
        List<Index> indexes = parseInputStrings(inputIndexStrings);

        // Act
        IndexOptimizer indexOptimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);


        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        Assertions.assertEquals(2, outputIndexStrings.size());
        Assertions.assertTrue(outputIndexStrings.contains("{{y,z}{x}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{x}{z}}"));
    }

    @Test
    @DisplayName("duplicates are removed")
    public void testWithMemoization2() {
        // Arrange
        List<Index> indexes = parseInputStrings(new String[]{
                "{{a,b}}",
                "{{a,b}}"
        });

        // Act
        IndexOptimizer optimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        List<Index> optimizedIndexes = optimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        assertThat(outputIndexStrings, hasSize(1));
        assertThat(outputIndexStrings, contains("{{a,b}}"));
    }

    @Test
    @DisplayName("duplicates are removed (reversed order)")
    public void testWithMemoization3() {
        // Arrange
        List<Index> indexes = parseInputStrings(new String[]{
                "{{a,b}}",
                "{{b,a}}"
        });

        // Act
        IndexOptimizer optimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        List<Index> optimizedIndexes = optimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        assertThat(outputIndexStrings, hasSize(1));
        assertThat(outputIndexStrings, contains("{{a,b}}"));
    }

    @Test
    @DisplayName("order restriction is enforced")
    public void testWithMemoization4() {
        // Arrange
        List<Index> indexes = parseInputStrings(new String[]{
                "{{a,c}}",
                "{{a}{b}{c}}"
        });

        // Act
        IndexOptimizer optimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        List<Index> optimizedIndexes = optimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        assertThat(outputIndexStrings, hasSize(2));
        assertThat(outputIndexStrings, containsInAnyOrder("{{a,c}}","{{a}{b}{c}}"));
    }

    @Test
    @DisplayName("optimization with empty field set")
    public void testWithMemoization5() {
        // Arrange
        List<Index> indexes = parseInputStrings(new String[]{
                "{{}{a,b}}",
                "{{b}{a}}"
        });

        // Act
        IndexOptimizer optimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        List<Index> optimizedIndexes = optimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        assertThat(outputIndexStrings, hasSize(1));
        assertThat(outputIndexStrings, containsInAnyOrder("{{b}{a}}"));
    }

    @Test
    @DisplayName("optimization with empty field set 2")
    public void testWithMemoization6() {
        // Arrange
        List<Index> indexes = parseInputStrings(new String[]{
                "{{b}{a}{}}",
                "{{}{a,b}{}}"
        });

        // Act
        IndexOptimizer optimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        List<Index> optimizedIndexes = optimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        assertThat(outputIndexStrings, hasSize(1));
        assertThat(outputIndexStrings, containsInAnyOrder("{{b}{a}}"));
    }

    @Test
    @DisplayName("possible optimization with overlapping constraints")
    public void test1() {
        // Arrange
        List<Index> indexes = parseInputStrings(new String[]{
                "{{a,b}{c,d}}",
                "{{b}{a,c}{d}}"
        });

        // Act
        IndexOptimizer optimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        List<Index> optimizedIndexes = optimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        assertThat(outputIndexStrings, hasSize(1));
        assertThat(outputIndexStrings, contains("{{b}{a}{c}{d}}"));
    }

    @Test
    @DisplayName("optimzal solution requires not covered with first contained-containing pair")
    public void test2() {
        // Arrange
        List<Index> indexes = parseInputStrings(new String[]{
                "{{1,2}{3}}",
                "{{2}{1}{3}}",
                "{{1}{2}}",
                "{{1}{2,3}}",
                "{{1}}"
        });

        // Act
        IndexOptimizer optimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        List<Index> optimizedIndexes = optimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        assertThat(outputIndexStrings, hasSize(2));
        assertThat(outputIndexStrings, containsInAnyOrder("{{2}{1}{3}}", "{{1}{2}{3}}"));
    }

    @Test
    @Disabled
    public void testWithSmallestIndexSetStrategy() {
        // Arrange
        String[] inputIndexStrings = {
                "{{x,y}}",
                "{{x,y,z}}",
                "{{z,x}}",
                "{{z,y}{x}}"
        };
        List<Index> indexes = parseInputStrings(inputIndexStrings);

        // Act
        IndexOptimizer indexOptimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        indexOptimizer.setMemoize(true);
        indexOptimizer.setShouldConsiderSkippedOptimizations(true);
        indexOptimizer.setIndexListSelectionStrategy(new SmallestIndexListSelectionStrategy());
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        Assertions.assertEquals(3, outputIndexStrings.size());
        Assertions.assertTrue(outputIndexStrings.contains("{{x,y}{z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{x,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{y,z}{x}}"));
    }

    @Test
    @Disabled
    public void testWithSmallestIndexSetStrategy2() {
        // Arrange
        String[] inputIndexStrings = {
                "{{1,0}}",
                "{{1,2,0}}",
                "{{2,0}}",
                "{{1,2}{0}}"
        };
        List<Index> indexes = parseInputStrings(inputIndexStrings);

        // Act
        IndexOptimizer indexOptimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        indexOptimizer.setMemoize(false);
        indexOptimizer.setMaskKeyNames(false);
        indexOptimizer.setShouldConsiderSkippedOptimizations(true);
        indexOptimizer.setIndexListSelectionStrategy(new SmallestIndexListSelectionStrategy());
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        Assertions.assertEquals(3, outputIndexStrings.size());
        Assertions.assertTrue(outputIndexStrings.contains("{{0,1}{2}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{0,2}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{1,2}{0}}"));
    }

    @Test
    @Disabled
    public void testWithSmallestIndexSetSelectionStrategy_03() {
        // Arrange
        String[] inputIndexStrings = {
                "{{a,d,f,g,j,n,r,t,z}}",
                "{{d,g,r}}",
                "{{a,z}}",
                "{{b,r}}",
                "{{g}}"
        };
        List<Index> indexes = parseInputStrings(inputIndexStrings);

        // Act
        IndexOptimizer indexOptimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        indexOptimizer.setIndexListSelectionStrategy(new SmallestIndexListSelectionStrategy());
        // consider skipped cases, otherwise some suboptimal cases will not be taken into account
        indexOptimizer.setShouldConsiderSkippedOptimizations(true);
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        Assertions.assertEquals(3, outputIndexStrings.size());
        Assertions.assertTrue(outputIndexStrings.contains("{{a,z}{d,f,g,j,n,r,t}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{g}{d,r}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{b,r}}"));
    }

    @Test
    public void testWithMinSumOfSquaresIndexListSelectionStrategy_03() {
        // Arrange
        String[] inputIndexStrings = {
                "{{a,d,f,g,j,n,r,t,z}}",
                "{{d,g,r}}",
                "{{a,z}}",
                "{{b,r}}",
                "{{g}}"
        };
        List<Index> indexes = parseInputStrings(inputIndexStrings);

        // Act
        IndexOptimizer indexOptimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        indexOptimizer.setIndexListSelectionStrategy(new MinSumOfSquaresIndexListSelectionStrategy());
        // consider skipped cases, otherwise some suboptimal cases will not be taken into account
        indexOptimizer.setShouldConsiderSkippedOptimizations(true);
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        Assertions.assertEquals(3, outputIndexStrings.size());
        Assertions.assertTrue(outputIndexStrings.contains("{{g}{d,r}{a,f,j,n,t,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{a,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{b,r}}"));
    }

    @Test
    @Disabled
    public void testWithLargestIndexSetSelectionStrategy_03() {
        // Arrange
        String[] inputIndexStrings = {
                "{{a,d,f,g,j,n,r,t,z}}",
                "{{d,g,r}}",
                "{{a,z}}",
                "{{b,r}}",
                "{{g}}"
        };
        List<Index> indexes = parseInputStrings(inputIndexStrings);

        // Act
        IndexOptimizer indexOptimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        indexOptimizer.setIndexListSelectionStrategy(new LargestIndexListSelectionStrategy());
        // consider skipped cases, otherwise some suboptimal cases will not be taken into account
        indexOptimizer.setShouldConsiderSkippedOptimizations(true);
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        List<String> outputIndexStrings = optimizedIndexes.stream()
                .map(Index::toStringSorted)
                .collect(Collectors.toList());
        Assertions.assertEquals(5, outputIndexStrings.size());
        Assertions.assertTrue(outputIndexStrings.contains("{{a,d,f,g,j,n,r,t,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{d,g,r}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{a,z}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{b,r}}"));
        Assertions.assertTrue(outputIndexStrings.contains("{{g}}"));
    }

    @Disabled("The test takes longer time to run, disable for CI builds")
    @Test
    public void compareTimesWithAndWithoutMemoization() {
        // Arrange
        String[] inputIndexStrings = {
                "{{a,d,f,g,j,n,r,t,z}}",
                "{{d,g,r}}",
                "{{a,z}}",
                "{{b,r}}",
                "{{g}}"
        };
        List<Index> indexes = parseInputStrings(inputIndexStrings);

        // Act
        IndexOptimizer indexOptimizer;
        long t1, t2;

        indexOptimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        indexOptimizer.setMemoize(false);
        t1 = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            indexOptimizer.optimizeIndexes(indexes);
        }
        t2 = System.currentTimeMillis();
        System.out.println("Execution time (without memoization): " + (t2 - t1));

        indexOptimizer = IndexOptimizer.createDefaultSingleThreadedOptimizer();
        indexOptimizer.setMemoize(true);
        t1 = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            indexOptimizer.optimizeIndexes(indexes);
        }
        t2 = System.currentTimeMillis();
        System.out.println("Execution time (with memoization):    " + (t2 - t1));
    }

    @Test
    public void test_large_with_short_index_names_01_limited_paths_per_step() {
        // Arrange
        List<Index> indexes = parseInputStrings(LONG_INPUT_INDEX_SET_STRINGS);
        printIndexes("Input", indexes);

        IndexOptimizer indexOptimizer = IndexOptimizer.createFastSingleThreadedOptimizer();

        // Act
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        Pair<List<Pair<Index, List<Index>>>, List<Index>> checkRes = runCoverageChecker(indexes, optimizedIndexes);
        Assertions.assertEquals(19, optimizedIndexes.size());
        Assertions.assertEquals(29, checkRes.getLeft().size());
        Assertions.assertTrue(checkRes.getRight().isEmpty());
    }

    @Test
    public void test_large_with_short_index_names_02_multithreaded_and_limited_paths_per_step() {
        // Arrange
        List<Index> indexes = parseInputStrings(LONG_INPUT_INDEX_SET_STRINGS);
        printIndexes("Input", indexes);

        IndexOptimizer indexOptimizer = IndexOptimizer.createFastMultiThreadedOptimizer();

        // Act
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        Pair<List<Pair<Index, List<Index>>>, List<Index>> checkRes = runCoverageChecker(indexes, optimizedIndexes);
        Assertions.assertEquals(19, optimizedIndexes.size());
        Assertions.assertEquals(29, checkRes.getLeft().size());
        Assertions.assertTrue(checkRes.getRight().isEmpty());
    }

    @Test
    public void test_large_with_long_index_names_01_multithreaded_and_limited_paths_per_step() {
        // Arrange
        List<Index> indexes = parseInputStrings(LONG_INPUT_INPUT_SET_LONG_STRINGS);
        printIndexes("Input", indexes);

        IndexOptimizer indexOptimizer = IndexOptimizer.createFastMultiThreadedOptimizer();

        // Act
        List<Index> optimizedIndexes = indexOptimizer.optimizeIndexes(indexes);
        printIndexes("Optimized", optimizedIndexes);

        // Assert
        Pair<List<Pair<Index, List<Index>>>, List<Index>> checkRes = runCoverageChecker(indexes, optimizedIndexes);
        Assertions.assertEquals(19, optimizedIndexes.size());
        Assertions.assertEquals(29, checkRes.getLeft().size());
        Assertions.assertTrue(checkRes.getRight().isEmpty());
    }

    private List<Index> parseInputStrings(String[] inputIndexStrings) {
        return Arrays.stream(inputIndexStrings)
                .map(Index::parseIndex)
                .collect(Collectors.toList());
    }

    private static void printIndexes(String setName, List<Index> indexes) {
        System.out.println(setName + ": Number of indexes: " + indexes.size());
        for (Index optimizedIndex : indexes) {
            System.out.println(optimizedIndex.toStringSorted());
        }
    }

    private static Pair<List<Pair<Index, List<Index>>>, List<Index>> runCoverageChecker(List<Index> indexes,
                                                                                        List<Index> optimizedIndexes) {
        IndexCoverageChecker checker = new IndexCoverageChecker();
        indexes = IndexOptimizer.sanitizeIndexes(indexes);
        Pair<List<Pair<Index, List<Index>>>, List<Index>> checkRes = checker.checkIndexCoverage(indexes,
                                                                                                optimizedIndexes);
        System.out.println(checkRes.getLeft());
        System.out.println(checkRes.getRight());
        return checkRes;
    }

}
