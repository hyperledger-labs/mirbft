# Producing figures with gnuplot
## Requirements

Install `gnuplot`:

`sudo apt-get install gnuplot-qt`

## Using `qnuplot`:
There are necessary components:

1. A file with the raw data (`.data` extension):
The file contains data in columns separated with spaces
2. A plotting script (`.plt` extension):
The script describes how the data form one or more data file will be plotted.

With the repository where the `.plt` scripts are as working directory, run the command `gnuplot`.

In the interactive console run `set terminal qt size 800, 450` to change the default aspect ratio.

Then run `load "file.plt"` where `file` the corresponding script name.

Exit the console with the command `q`.

## Producing the figures from mir-BFT paper

* Figure `5a`: `load "WAN-small.plt"`
* Figure `5b`: `load "WAN-Hashes.plt"`
* Figure 6:    `load "WAN-3500.plt"`
* Figure 7: `load "LAN-500.plt"`
* Figure 8a: `load "LT-500.plt"`
* Figure 8b: `load "LT-3500.plt"`
* Figure 9: `load "WAN-duplicates.plt"`
* Figure 11a: `load "censor.plt"`
* Figure 11b: `load "cdf.plt"`