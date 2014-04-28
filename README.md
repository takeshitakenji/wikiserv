wikiserv
========

Wiki server with manual editing of text files as backend, with a selectable filter
converting them to HTML and a caching mechanism.


Justification
-------------

I've tried using purely web-based wikis like [MediaWiki](http://www.mediawiki.org/wiki/MediaWiki]),
but the database and server maintenance involved with them have always ended up
leaving me neglecting them.  This time, I'm going to try something a bit
different.

Requirements
------------
1. Python 3 (3.3 or newer)
2. [python-dateutil](http://labix.org/python-dateutil)

Design
------

1. File locking everywhere to keep things consistent _within_ the server processes
   and threads.

2. Raw source files will be used as the input, which can be modified whenever.

3. A caching system with a directory tree that corresponds to the source
   (asciidoc, etc.) structure.

   * Web server processes will have a shared lock on a toplevel `.lock`
     file.
   * Other threads and processes will have an exclusive lock on a toplevel
     `.lock` file.

4. The caching system will have two methods of cleanup.

   1. A time to live system that with a configurable maximum age.  The
      cleanup process will have to be scheduled in cron or something
      similar.
   2. An optional LRU-based system that will delete the oldest entry
      when there are too many sitting around.  A thread will run in the
      background will have this task dispatched to it.

5. The caching system will use file size, file modification time, and a
   configurable checksum to check for changes in source files.

6. The actual filter will be configurable and replaceable, with
   [asciidoc](http://www.methods.co.nz/asciidoc/) as both the initial
   and reference implementation.

7. Any source file revision control is left to the person managing the
   source directory tree.
