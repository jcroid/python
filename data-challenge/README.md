
# Takehome Test (Data)

## Overview

This is a takehome test for candidates applying for a Data Engineer
position at MoonPay.

Feel free to solve these questions however you see fit, using whatever coding
style or third-party libraries you think are appropriate.

To start the test, simply clone this repo and make your edits locally.

## Project

### Introduction

Every day at MoonPay, we receive data from various providers (e.g.
acquiring banks, cryptocurrency exchanges). We use this data mainly for
accounting and data reconciliation.

You can find two data samples in this repository:

1. [A CSV file from one of our acquiring bank](data/sample_acquirer_a.csv)
2. [A JSON file from another acquirer](data/sample_acquirer_b.json)

For further analysis (e.g. visualizations in a web app or simulations done by
another microservice), the following requirements need to be met.

### Requirements

- Automated or on-demand processes to ingest and store the data
- Test the quality of the data
- A service that is able to provide the following functionality:
  - Return the total amount processed by acquirer and by currency for a given time range
  - Inform about the status of the data ingestion
- The solution should be written in Python using a suitable database.
- Please include instructions on how to build and run.

## Follow-up

Answer the questions in the [FOLLOW-UP.md](./FOLLOW-UP.md) file.


