#!/bin/bash

output=$1
platform=$2

rm -f "$output"

pex . -v -c main.py -r requirements.txt -o "$output" --platform "$platform"

read -p "Press enter to continue"
