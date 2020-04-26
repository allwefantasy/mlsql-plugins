## Install 

```sql
!plugin app add - echo-controller-2.4;
```

## Usage

```
select crawler_http("http://127.0.0.1:9003/run/script","POST",map("owner","wow","sql","select 1 as a as output;","executeMode","echo")) as c as output;
``` 

The server will response with `select 1 as a as output;` back instead of execute the sql. 