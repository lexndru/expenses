# expenses

An oversimplified library to work with expenses.

The library is implemented with two principles in mind:
- Records must be treated as collection of immutable items (except for their metadata) and can only be created;
- Records must be available for listing.

The library merely hints to respect these principles and does not limit or enforce application behaviour.

```
$ go test -cover
PASS
coverage: 90.8% of statements
ok  	github.com/lexndru/expenses	0.705s
```

MIT License
