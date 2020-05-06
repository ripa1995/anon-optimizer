package extractor;

import java.util.ArrayList;

public class QueryUtility {

    private ArrayList<String> filterColumn;
    private ArrayList<String> selectColumn;
    private ArrayList<String> havingColumn;
    private ArrayList<String> groupColumn;
    private int priority;

    public QueryUtility(ArrayList<String> filterColumn, ArrayList<String> selectColumn, ArrayList<String> havingColumn, ArrayList<String> groupColumn, int priority) {
        this.filterColumn = filterColumn;
        this.selectColumn = selectColumn;
        this.havingColumn = havingColumn;
        this.groupColumn = groupColumn;
        this.priority = priority;
    }

    public ArrayList<String> getFilterColumn() {
        return filterColumn;
    }

    public void setFilterColumn(ArrayList<String> filterColumn) {
        this.filterColumn = filterColumn;
    }

    public ArrayList<String> getSelectColumn() {
        return selectColumn;
    }

    public void setSelectColumn(ArrayList<String> selectColumn) {
        this.selectColumn = selectColumn;
    }

    public ArrayList<String> getHavingColumn() {
        return havingColumn;
    }

    public void setHavingColumn(ArrayList<String> havingColumn) {
        this.havingColumn = havingColumn;
    }

    public ArrayList<String> getGroupColumn() {
        return groupColumn;
    }

    public void setGroupColumn(ArrayList<String> groupColumn) {
        this.groupColumn = groupColumn;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }
}
