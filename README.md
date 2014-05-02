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
2. [pytz](http://pytz.sourceforge.net/)
3. [python-dateutil](http://labix.org/python-dateutil)
4. [lxml](http://lxml.de/)
5. [tornado](http://www.tornadoweb.org/)
6. An OS that supports the built-in [fcntl](http://docs.python.org/3.3/library/fcntl.html) module

Configuration
-------------


```xml
<?xml version="1.0" ?>

<configuration>
	<log-level>DEBUG</log-level><!-- Passed to logging module -->
	<cache>
		<cache-dir>testdata/test_cache</cache-dir><!-- Root of cache directory -->
		<source-dir>testdata/test_root</source-dir><!-- Root of directory containing files which will be procesed and served -->
		<checksum-function>sha1</checksum-function><!-- Checksum algorithm used on the files to be processed to determine cache state -->
		<max-age>86400</max-age><!-- OPTIONAL: Whenever a scrub is performed, delete files that are older than this age (seconds) -->
		<max-entries>2048</max-entries><!-- OPTIONAL: Use an LRU algorithm to limit the approximate maximum number of entries in the cache -->
		<auto-scrub /><!-- OPTIONAL: When the LRU algorithm hits the maximum number of entries, automatically scrub the cache to clear up free slots -->
		<dispatcher-threar /><!-- OPTIONAL: Use the DispatcherCache class instead, which will perform automatic scrubbing in a separate thread -->
		<send-etags /><!-- OPTIONAL: Send Etags based on checksum algorithm -->
	</cache>
	<processors>
		<encoding>utf8</encoding><!-- Output encoding passed to all the processors -->
		<processor>asciidoc-xhtml11</processor><!-- Sets the default processor used to convert files to HTML -->
		<processor extensions="txt foo">asciidoc-xhtml11</processor><!-- For the extensions txt and foo, use this processor to convert -->
		<processor extensions="bar">asciidoc-html5</processor><!-- For the extensions bar, used asciidoc-html5 instead -->
	</processors>
</configuration>
```


Design
------

1. File locking everywhere to keep things consistent _within_ the server processes
   and threads.

2. Raw source files will be used as the input, which can be modified whenever.

3. A caching system with a directory tree that corresponds to the source
   (asciidoc, etc.) structure.

    1. Web server processes will have a shared lock on a toplevel `.lock`
       file.
    2. Other threads and processes will have an exclusive lock on a toplevel
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
