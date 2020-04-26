## Install 

```sql
!plugin et add - "run-script-2.4" named runScript;
```

## Usage

```sql
set code1='''
select 1 as a as b;
''';
!runScript '''${code1}''' named output;
```