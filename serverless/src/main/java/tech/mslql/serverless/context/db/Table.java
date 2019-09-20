package tech.mslql.serverless.context.db;

/**
 * 2019-09-19 WilliamZhu(allwefantasy@gmail.com)
 */
public class Table {
    private String db;
    private String table;

    public Table(String db, String table) {
        this.db = db;
        this.table = table;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
