# LogCausalAnalysis

## Overview

This project provides a series of functions to analyze 
system log data in terms of event causality.

* Classify log data with its output format
* Generate DAG with PC algorithm (using pcalg/gsq package)
* Process log incrementally and notify troubles <- work in progress

## Package requirements

pcalg https://github.com/keiichishima/pcalg
gsq https://github.com/keiichishima/gsq

## Tutorial

You can generate pseudo log dataset for testing functions.

$ python testlog.py > test.temp

First, you need to define log templates for dataset.
(If you have completed log template set, you can import it with lt_manager.)

$ python lt_generate_shiso.py va test.temp

Then classify dataset and register them with database.

$ python log_db.py test.temp

Finally, analyze causal relations generating DAG.
(This step requires much time. If your machine have enough performance,
we recommend you to use -p options for multithreading.)

$ python pc_log.py

You can check result DAG with following command.

$ python pcresult.py -g graph.pdf pc_output/all_21120901

