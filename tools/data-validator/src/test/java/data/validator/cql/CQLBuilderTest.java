package data.validator.cql;


import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static data.validator.constants.Constants.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by Tarek on 5/4/2017.
 */
public class CQLBuilderTest {

  private final static String EXPECTED_PRIMARY_KEYS_SELECTION_STATEMENT = "SELECT key1 , key2 , key3 from sourceTable;";
  private final static String EXPECTED_COUNT_STATEMENT = "Select count(*) from sourceTable WHERE key1= ? AND key2= ? AND key3= ? ;";
  private static List<String> primaryKeys;


  @BeforeClass
  public static void init() {
    primaryKeys = Arrays.asList("key1", "key2", "key3");
  }

  @Test
  public void shouldReturnTheExpectedPrimaryKeysSelectionCQL() {
    assertEquals(EXPECTED_PRIMARY_KEYS_SELECTION_STATEMENT,
        CQLBuilder.constructSelectPrimaryKeysFromSourceTable(SOURCE_TABLE, primaryKeys));
  }

  @Test
  public void shouldReturnTheExpectedCountStatement() {
    assertEquals(EXPECTED_COUNT_STATEMENT, CQLBuilder.getMatchCountStatementFromTargetTable(SOURCE_TABLE, primaryKeys));
  }


}