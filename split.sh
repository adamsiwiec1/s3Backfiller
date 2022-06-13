#!/bin/bash
filename="$1"
filetype="$2"
parts="$3"
cd tmp/upload/
sleep 3

#for ((i = 1; i <= $parts; i++));
#do
#    dd if=$filename"$filetype" of=$filename"$i"$filetype bs=1024k skip=$[i*100 - 100] count=100;
#done

for i in {1..100};
  do
    echo i
    dd if=$filename"$filetype" of=$filename"$i"$filetype bs=1024k skip=$[i*100 - 100] count=100;
done