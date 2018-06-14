package data.validator.cql;

import java.util.List;

/**
 * Created by Tarek on 4/27/2017.
 */
public class CQLBuilder {
    public static String getMatchCountStatementFromTargetTable(String targetTableName, List<String> names) {
        String wherePart = getStringWherePart(names);
        StringBuilder selectStatementFromTargetTableBuilder = new StringBuilder();
        selectStatementFromTargetTableBuilder.append("Select count(*) from ").append(targetTableName).append(" WHERE ").append(wherePart);
        return selectStatementFromTargetTableBuilder.toString();
    }

    private static String getStringWherePart(List<String> names) {
        StringBuilder stringBuilderForWherePart = new StringBuilder();
        for (int i = 0; i < names.size() - 1; i++)
            stringBuilderForWherePart.append(names.get(i)).append("= ? AND ");
        stringBuilderForWherePart.append(names.get(names.size() - 1)).append("= ? ;");
        return stringBuilderForWherePart.toString();
    }

    public static String constructSelectPrimaryKeysFromSourceTable(String sourceTableName, List<String> primaryKeyNames) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("SELECT ");
        for (int i = 0; i < primaryKeyNames.size() - 1; i++)
            stringBuilder.append(primaryKeyNames.get(i)).append(" , ");
        stringBuilder.append(primaryKeyNames.get(primaryKeyNames.size() - 1));
        stringBuilder.append(" from ").append(sourceTableName).append(";");
        return stringBuilder.toString();
    }
}
