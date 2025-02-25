#!/bin/bash

python -m unit_of_work --tape-name XXX &
python -m unit_of_work --tape-name XXX &
python -m unit_of_work --tape-name XXX &
python -m unit_of_work --tape-name XXX &
python -m unit_of_work --tape-name XXX &

wait

echo "All parallel runs have finished execution!"