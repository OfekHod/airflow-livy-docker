#!/bin/bash

for py_file in $(find airflowlivy/dags -name "*.py" | grep -v __init__.py)
do
    echo Running: python $py_file
    python $py_file
done
