#!/bin/sh


rsync -av ~/src/ghetto-cluster/*.py bucko:~/gc/
rsync -av ~/src/ghetto-cluster/*.py mc-wifi:~/gc/
rsync -av ~/src/ghetto-cluster/*.py ~/gc/

exit
rsync -av ~/src/ghetto-cluster/*.py node0:~/gc/
rsync -av ~/src/ghetto-cluster/*.py node1:~/gc/
rsync -av ~/src/ghetto-cluster/*.py imac:~/gc/
rsync -av ~/src/ghetto-cluster/*.py mc-wifi:~/gc/
