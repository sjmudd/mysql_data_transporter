# mysql_data_transporter

Go code to help transport data between MySQL servers more efficiently

TASK: to successfully and efficiently copy data from one server to another.

* should be vendor nuetral if possible so should work between different vendors
  and major versions where possible.
* mysqldump doesn't work efficiently (too slow)
* mysqlpump works for 5.7 and later
* pt-archiver looked good but if you're not careful it'll delete your data (groan)
* I want to do this by pulling data from the source server in
  parallel (configurable) in chunks and push to the destination server in chunks.
* there are parcial tools for doing this but nothing that's complete
* Other options that might be of interest: do this by renaming
  tables and making blackhole tables and inserting into the bh tables
  generating binlogs (RBR format)
* data size is expected to be large. Say 500GB plus.
* do not store intermediate data on disk: we want to avoid this
