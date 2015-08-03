#!/bin/sh

dirpath=$1
for path in `cat env.txt`
do
	scp $path $dirpath
done

