## Install 

```sql
!plugin et add tech.mlsql.plugins.et.TableRepartition "table-repartition";
```

## Usage

```sql
run table as TableRepartition.`` where partitionNum="10" and partitionType="range" and partitionCols="id"
as newtable;
```