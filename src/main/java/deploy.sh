#!/bin/bash
# A simple variable example
login="mmanner-21"
remoteFolder="/tmp/$login/"
fileName="SimpleServerProgram"
fileExtension=".java"
#computers=("tp-1a226-01" "tp-1a226-02" "tp-1a226-03" "tp-1a226-04" "tp-1a226-05")
computers=("tp-1a226-06.enst.fr" "tp-1a226-07.enst.fr" "tp-1a226-08.enst.fr"  "tp-1a226-12.enst.fr" "tp-1a226-14.enst.fr" "tp-1a226-16.enst.fr" "tp-1a226-17.enst.fr" "tp-1a226-20.enst.fr" "tp-1a226-22.enst.fr" "tp-1a226-23.enst.fr")
#computers=("tp-1a226-06.enst.fr")
for c in ${computers[@]}; do
#    command0=("ssh" "$login@$c" "lsof -ti | xargs kill -9")
  command0=("ssh" "$login@$c" "lsof -ti | xargs kill -9;rm -rf $remoteFolder;mkdir $remoteFolder")
  command2=("scp" "$fileName$fileExtension" "$login@$c:$remoteFolder$fileName$fileExtension")
  command3=("ssh" "$login@$c" "cd $remoteFolder;javac $fileName$fileExtension; java $fileName")
#  echo ${command0[*]}
  "${command0[@]}"
#  echo ${command1[*]}
#  "${command1[@]}"
#  echo ${command2[*]}
  "${command2[@]}"
  echo ${command3[*]}
  "${command3[@]}" &
done