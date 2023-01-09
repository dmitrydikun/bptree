# B+ Tree implementation for Go

[![GoDoc](https://godoc.org/github.com/dmitrydikun/bptree?status.svg)](https://godoc.org/github.com/dmitrydikun/bptree)

This package provides an in-memory B+ Tree implementation for Go. Only unique keys are supported in current version. Not thread-safe.

Supported operations:
- Insert
- Find
- Delete (Eject)
- Iterating
- Range

TODO:
- Duplicated keys
- Bulk initialization
