# Table: github_my_gist

GitHub Gist is a simple way to share snippets and pastes with others. The `github_my_gist` table will list only gists that **you own**.

To query **ANY** gist that you have access to (including any public gists), use the `github_gist` table.

## Examples

### List your gists

```sql
select
  *
from
  github_my_gist;
```

### List your public gists

```sql
select
  *
from
  github_my_gist
where
  public;
```

### Summarize your gists by language.

```sql
select
  file ->> 'language' as language,
  count(*)
from
  github_my_gist g
cross join
  jsonb_array_elements(g.files) file
group by
  language
order by
  count desc
```
