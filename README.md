# pathway-indexer

[![Release](https://img.shields.io/github/v/release/DallanQ/pathway-indexer)](https://img.shields.io/github/v/release/DallanQ/pathway-indexer)
[![Build status](https://img.shields.io/github/actions/workflow/status/DallanQ/pathway-indexer/main.yml?branch=main)](https://github.com/DallanQ/pathway-indexer/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/DallanQ/pathway-indexer/branch/main/graph/badge.svg)](https://codecov.io/gh/DallanQ/pathway-indexer)
[![Commit activity](https://img.shields.io/github/commit-activity/m/DallanQ/pathway-indexer)](https://img.shields.io/github/commit-activity/m/DallanQ/pathway-indexer)
[![License](https://img.shields.io/github/license/DallanQ/pathway-indexer)](https://img.shields.io/github/license/DallanQ/pathway-indexer)

Create and maintain the index for the BYU Pathway service missionary chatbot

- **Github repository**: <https://github.com/DallanQ/pathway-indexer/>
- **Documentation** <https://DallanQ.github.io/pathway-indexer/>

## Getting started with your project

First, install the environment and the pre-commit hooks with

```bash
make install
```

### Download the data

#### Setup

1. get the `interns.pem` file from Dallan and copy it to `~/.ssh/interns.pem`
2. `chmod 400 ~/.ssh/interns.pem`
3. edit `~/.ssh/config` and add the following lines
```
Host 35.90.214.49
  HostName 35.90.214.49
  User ec2-user
  IdentityFile ~/.ssh/interns.pem
```
4. `ssh 35.90.214.49` to make sure you can get into the machine with the shared data directory. If asked a yes/no question about signing in, answer Yes.
5. `make pull-data` to pull data from the shared data directory into your local `data` directory (omit this step for now).

#### Usage

The `data` directory is now special. 
It is excluded from git (see .gitignore) and is only handled by make push-data and pull-data. 
This gives us a way to share large files that git will complain about.

- `make pull-data` to pull the data from the shared data directory to your local `data` directory.
- `make push-data` to push the data from your local `data` directory to the shared data directory.

The shared data directory is just a regular directory.
It doesn't have version control. 
Because of this, it's generally a good idea to add date-stamps to your filenames so you don't accidentally overwrite files. 

Finally, if you push something by accident and want to delete it, you need to ssh into the 35.90.214.49 box and cd to /interns/pathway to delete it from the shared directory.

## Weekly: Load new data

create a new directory in the data folder (with today's date)
add that directory to the .env file (DATA_PATH)

### Run Crawler

```bash
poetry shell
python main.py
```

### Load the data into the index

???
