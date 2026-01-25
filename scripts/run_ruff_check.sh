#!/bin/bash

# formatting
ruff format --check
RESULT1=$?


## linting
ruff check
RESULT2=$?


## sorting Imports
ruff check --select I
RESULT3=$?


if [[ "$RESULT1" -eq 0 && "$RESULT2" -eq 0 && "$RESULT3" -eq 0 ]]; then
    printf "\nRuff Formatter, Linter, Import Sorting Test\n\n-- PASSED --\n\n"
    exit 0
else
    printf "\nRuff Formatter, Linter, Import Sorting Test\n\n-- FAILED --\n\n"
    exit 1
fi