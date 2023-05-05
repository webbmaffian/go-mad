# Go mad
Memory-mapped ([mmap](https://en.wikipedia.org/wiki/Memory-mapped_file)) abstract data types ([ADT](https://en.wikipedia.org/wiki/Abstract_data_type)) for Go. All data types persist to disk and use only what memory is available with mmap, thus allowing bigger-than-memory data types.

- Array ([mmarr](./mmarr))
- Hash table ([hashmmap](./hashmmap))

---

**DISCLAIMER: These packages are not yet stable and are subject to change.**