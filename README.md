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

First, install the environment and the pre-commit hooks with:

```bash
make install
```

This will:

- Create a virtual environment using Poetry
- Install dependencies
- Install Git pre-commit hooks

Note: If using Poetry 2.x, the `poetry shell` command is not included by default. Instead:

```bash
# Recommended: use this to run commands
poetry run python main.py

# Or manually activate the environment (optional for interactive use):
source $(poetry env info --path)/bin/activate
```

---

## Download the data

### Setup

1. Get the `interns.pem` file from Dallan and copy it to `~/.ssh/interns.pem`
2. Run: `chmod 400 ~/.ssh/interns.pem`
3. Edit your `~/.ssh/config` and add the following lines:

```ssh
Host 35.90.214.49
  HostName 35.90.214.49
  User ec2-user
  IdentityFile ~/.ssh/interns.pem
```

4. `ssh 35.90.214.49` to make sure you can get into the machine with the shared data directory. If asked a yes/no question about signing in, answer Yes.

5. `make pull-data` to pull data from the shared data directory into your local `data` directory (omit this step for now).

### Usage

The `data` directory is now special.
It is excluded from git (see .gitignore) and is only handled by make push-data and pull-data.
This gives us a way to share large files that git will complain about.

- `make pull-data` to pull the data from the shared data directory to your local `data` directory.
- `make push-data` to push the data from your local `data` directory to the shared data directory.

The shared data directory is just a regular directory.
It doesn't have version control.
Because of this, it's generally a good idea to add date-stamps to your filenames so you don't accidentally overwrite files.

Finally, if you push something by accident and want to delete it, you need to ssh into the 35.90.214.49 box and cd to /interns/pathway to delete it from the shared directory.

---

## Weekly: Load new data

1. Create a new subdirectory in the `data/` folder (with today's date, e.g. `adata_07_10_25/`)
2. Update your `.env` file with the new path: `DATA_PATH=data/adata_07_10_25`

### Run crawler

```bash
poetry run python main.py
```

### Load the data into the index

```bash
poetry run python store.py
```

---
