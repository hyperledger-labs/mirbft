#! /bin/bash

pkill discoverymaster
pkill discoveryslave
pkill orderingclient
pkill orderingpeer
rm -r experiment-output-local/
go install ../...
