#!/bin/sh -e

URL="http://localhost:8080/report"
NUM_POINTS=25
INTERVAL_SEC=300
SOURCES="living_room top_room"
NAMES="temperature humidity"
VALUE_START=30
VALUE_SOURCE_INCREMENT=5
VALUE_TIME_INCREMENT=1

now=$(date +%s)
time=$((${now} - ${NUM_POINTS} * ${INTERVAL_SEC}))

for i in $(seq 0 "${NUM_POINTS}"); do
  data=
  time=$((${time} + ${INTERVAL_SEC}))
  value=$((${VALUE_START} + ${i} * ${VALUE_TIME_INCREMENT}))
  for source in ${SOURCES}; do
    value=$((${value} + ${VALUE_SOURCE_INCREMENT}))
    for name in ${NAMES}; do
      [ -z "${data}" ] || data="${data}%0A"
      data="${data}${time}|${source}|${name}|${value}"
    done
  done
  curl --data "d=${data}" -X POST "${URL}"
done
