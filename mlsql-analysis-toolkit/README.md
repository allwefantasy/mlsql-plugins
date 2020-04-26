## Install command:

```
!plugin app add - "mlsql-analysis-toolkit-2.4";
```


## Usage

To compute field in table the medium number.

```sql
!approxQuantile time_temp birthday "0.5" valued time_quantile;
select ${time_quantile} as quantile as output;
```





