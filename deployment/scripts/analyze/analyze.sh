#!/bin/bash
#
# If ran on MacOS, the gdate (Linux-style date) command is installed (part of coreutils).
# Using Homebrew, it can be installed by running
#   brew install coreutils

dbfile=eventDB.sqlite
queryOutput=performance

queries=
forcePeerBinary=
forceClientBinary=
forceDB=false
forceQueries=false
forceProfile=false
deleteRawData=false

function analyze() {
  dir=$1
  skipping=true

  echo "Analyzing: $dir"

  if $forceDB || [ ! -r "$dir/$dbfile" ]; then
    echo "  > Loading trace into database..."
    startTimeNs=$(gdate +%s%N 2>/dev/null || date +%s%N) # This is due to a different date command on Mac.

    python3 scripts/analyze/load-logs.py "$dir/$dbfile" $dir/slave-*/*.trc #the last argument must not be quoted!

    endTimeNs=$(gdate +%s%N 2>/dev/null || date +%s%N) # This is due to a different data command on Mac.
    echo "  > Loaded trace into database in $(((endTimeNs - startTimeNs) / 1000000000)) s."
    skipping=false
  fi

  if $forceQueries || [ ! -r "$dir/$queryOutput" ]; then
    rm -f ${dir:?}/$queryOutput

    for queryFile in $queries; do
      echo "  > Running queries on trace database: $queryFile"
      startTimeNs=$(gdate +%s%N 2>/dev/null || date +%s%N) # This is due to a different data command on Mac.

      python3 scripts/analyze/run-queries.py "$dir/$dbfile" "$queryFile" "$dir" >> $dir/$queryOutput

      endTimeNs=$(gdate +%s%N 2>/dev/null || date +%s%N) # This is due to a different data command on Mac.
      echo "  > Processed '$queryFile' in $(((endTimeNs - startTimeNs) / 1000000000)) s."

      skipping=false
    done
  fi


  if $skipping; then
    echo "  > Nothing to do. Skipping."
  fi
}

while [ -n "$1" ]; do

  # Peer binary
  if [ "$1" = "-b" ]; then
    shift
    forcePeerBinary=$1
  # Client binary
  elif [ "$1" = "-c" ]; then
    shift
    forceClientBinary=$1
  # Delete raw data when done (even if failed, use with care!!!)
  elif [ "$1" = "-d" ]; then
    deleteRawData=true
  # SQL query file
  elif [ "$1" = "-q" ]; then
    shift
    queries="$queries $1"
  # Force analysis even when results present
  elif [ "$1" = "-f" ] || [ "$1" = "--force" ]; then
    shift
    if [ "$1" = "db" ]; then
      forceDB=true
    elif [ "$1" = "queries" ]; then
      forceQueries=true
    elif [ "$1" = "profile" ]; then
      forceProfile=true
    elif [ "$1" = "all" ]; then
      forceDB=true
      forceQueries=true
      forceProfile=true
    else
      echo "Unknown argument to -f / --force option: $1. Arguments allowed: db queries all profile" >&2
    fi
  # Analyze directory
  else
    analyze "$1"

    if $deleteRawData; then
      rm -r ${dir:?}/slave-*/ ${dir:?}/$dbfile
    fi
  fi

  shift
done
