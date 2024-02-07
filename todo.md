# Gribjump Todo

### Tests
- [ ] Cleanup existing tests
- [ ] Tests for local part of the serverside (including multithreading, etc.)
- [ ] Test suite?

### Cache
- [ ] Fix slow reading of cache from disk.
- [ ] Automated generation of cache.
- [ ] Automated cleaning up cache.
- [ ] Multiprocess cache consistency.

### Review
- [ ] Review API (remove unwanted functions, consider using URI for location)
- [ ] Review class structure in decoders.

### Optimisation
- [ ] Optimise serialisation of requests/replies etc. between client and server.
- [ ] Requests in the form of a tree

### Functionality
- [ ] Compatible with RemoteFDB
- [ ] Introduce abstractions for accessing data cube and codex.
- [ ] Better suppport for mars requests with both cardinality == 1 and > 1.

### Misc.
- [ ] Rename tools to gribjump-verb
- [ ] Tool for comparing with eccodes should be able to handle requests of different cardinality.

### Publish
- [ ] Publish code for public use
- [ ] Paper related to this? (~for december 2024)
- [ ] Documentation