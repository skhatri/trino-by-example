#!/usr/bin/env bash

schematool -dbType postgres -validate
if [[ $? -ne 0 ]];
then
  schematool -dbType postgres  -initSchema
fi;

hiveserver2 --service metastore &
hiveserver2