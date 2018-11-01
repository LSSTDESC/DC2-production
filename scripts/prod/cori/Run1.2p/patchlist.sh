#!/bin/bash

echo "arg=$1"
shifter --image=$STACK_VERSION -- "./patchlist.run" $1

#run1.2p: 4429  4430  4431  4432  4433  4636  4637  4638  4639  4640  4848  4849  4850  4851  4852  5062  5063  5064  5065  5066
