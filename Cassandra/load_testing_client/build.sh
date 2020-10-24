#!/bin/bash

outputPex=$1

# Для Linux можно использовать платформу linux-x86_64-cp-38-cp38
platform=$2

rm -f "$outputPex"

pex . -v -c main.py -r requirements.txt -o "$outputPex" --platform "$platform"

read -p "Press enter to exit"
