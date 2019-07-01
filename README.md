# LogCausalAnalysis

## Important Notice

This project will not be updated in the future.
Instead, consider to use [amulog](https://github.com/cpflat/amulog) and [logdag](https://github.com/cpflat/logdag)
which provides equivalent functions, improved interface, and newer tools.
This project will be left as is for validating our previous published paper (see [Reference](#Reference)).

## Overview

This project provides a series of functions to analyze 
system log data in terms of event causality.

* Classify log data with its output format
* Generate DAG with PC algorithm (using pcalg/gsq package)
* Process log incrementally and notify troubles <- work in progress

## Package requirements

* pcalg https://github.com/keiichishima/pcalg
* gsq https://github.com/keiichishima/gsq

## Tutorial

You can generate pseudo log dataset for testing functions.

```
$ python testlog.py > test.temp
```

First, you need to generate a configuration file for whole system.
Copy sample file, and edit it if necessary.

```
$ cp config.conf.sample config.conf
```

Then classify dataset and register them with database.
Classification works with log template generation inside this command.

```
$ python log_db.py -c config.conf make
```

You can see log templates found in log messages with following command.

```
$ python log_db.py -c config.conf show-lt
```

Then analyze causal relations generating DAG.
(This step requires much time. If your machine have enough performance,
we recommend you to use -p options for multithreading.)

```
$ python pc_log.py
```

You can check result DAG with following command.

```
$ python pcresult.py -g graph.pdf show pc_output/all_21120901
```


## Reference

This project is evaluated in a [paper](https://doi.org/10.1109/TNSM.2017.2778096).
If you use this code, please consider citing:
```
@article{Kobayashi2018,
  author = {Kobayashi, Satoru and Otomo, Kazuki and Fukuda, Kensuke and Esaki, Hiroshi},
  journal = {IEEE Transactions on Network and Service Management},
  volume = {15},
  number = {1},
  pages = {53-67},
  title = {Mining causes of network events in log data with causal inference},
  year = {2018}
}
```

## License

3-Clause BSD license

## Author

Satoru Kobayashi

