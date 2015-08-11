#!/bin/sh

set -eux

sh clean.sh
python testlog.py > test.temp
python log_db.py test.temp
python pc_log.py -p 4
python pcresult.py -g graph.pdf pc_output/all_21120901

