#!/bin/bash
for ((i=1;i<=200;i++));
do
	echo "ROUND $i";
	go test  > ./out/out-$i.txt;
done
grep "FAIL" out/out-*
