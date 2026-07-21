
Overview of the inner Machinery
===============================


GribJump is a multi-threaded application. 

For a given request in the mars language, GribJump first performs an ``fdb.list`` to obtain the URIs of each of the fields.
It translates this into a list of files and byte offsets to be read.
GribJump has a pool of worker threads that read the files in parallel, and decode the data.
