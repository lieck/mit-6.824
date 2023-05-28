#!/bin/bash
for ((i=1;i<=50;i++));
do
	echo "ROUND $i";
	go test --run TestLots > ./out/out-$i.txt;
	go test --run TestPartition > ./out/out-1-$i.txt;
	go test --run TestManyUnreliable > ./out/out-2-$i.txt;
	go test --run TestOld > ./out/out-3-$i.txt;
	go test --run TestMany > ./out/out-4-$i.txt;
done
grep "FAIL" out/out-*
