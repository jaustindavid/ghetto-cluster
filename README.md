# ghetto-cluster
A ghetto file clustering package (and a way to learn Python).

Uses ssh + rsync to communicate; on startup, will read config.txt 
(or specified file), attempt to copy a clean version from the 
configured source config (if not local), then will basically
execute "rsync source replica", and later build a JSON list of all
sync'd files and stat(3) info.  Compares the stat lists between
source and replicae to determing what's complete, current, etc.

Most of this functionality could be reimplemented with a ~ dozen
lines of shell.  



# config-test.txt like
source config: srchost:~/gc/config-test.txt
ignore suffix: .DS_Store, .fseventsd, .Spotlight-V100, .DocumentRevisions-V100,
.sync
lazy write: 10
cycle: 21600
logfile: ~/gc/gc.log
rsync options: timeout=180
testing: True

source: bucko:/home/austin/gc/test-data
replica: mini-pi:/tmp/test-data

source: srchost:/path/to/files
replica: dsthost1:/mnt/files
replica: dsthost2:/mnt/files
ignore suffix: mobile, index.html, streams
rsync options: bwlimit=10m, delete-after

source: srchost:/path/to/morefiles
replica: dsthost1:/mnt/morefiles
rsync options: delete

source: srchost:/path/to/morefiles
replica: dsthost2:/mnt/morefiles
ignore suffix: doc
