# Scylla Basic Migrator

This python code is designed to do a basic single table migration from one environment to another. This can also be used for migrating an existing table from Cassandra to Scylla.

## Disclaimer

This code has no error handling and comes with no guarantees. It serves as a sample to work with and enhance accordingly to ensure it matches your requirements.

## Requirements

This code is designed to run on a dedicated machine with at least 8 vCPU available for better performance. The node should have connectivity to `CQL` port on both source and the target clusters.

### Python & pip

Python and pip should be available on the machine where this code is going to execute. Create a python virtual environment using the following 

```
python -m venv venv
source venv/bin/activate
```

Remember which folder you created the virtual environment in, every time we log out and log in, we need to `source venv/bin/activate` to reload the virtual environment. Once the virtual environment is active, install the `requirements.pkg` from this repository by using. Take note of the path of the `requirements.pkg` file and execute the following from within the folder. This will install the required packages within the virtual environment without effecting your global computer's environment. 

```
❯ pip install -r requirements.pkg
```

## Configuration

create a `.env` file based on the `.env_sample` and define the following variables according to your setup

```
# Source Cluster Configuration
SOURCE_CONTACT_POINTS=<SOURCE-IP1, SOURCE-IP2, ...>
SOURCE_PORT=<source-port>
SOURCE_USERNAME=<source_username>
SOURCE_PASSWORD=<source_password>

# Target Cluster Configuration
TARGET_CONTACT_POINTS=<SOURCE-IP1, SOURCE-IP2, ...>
TARGET_PORT=<target_port>
TARGET_USERNAME=<target_username>
TARGET_PASSWORD=<target_password>

# Migration Settings
BATCH_SIZE=500
PARALLEL_THREADS=16
KEYSPACE=my_keyspace
TABLE_NAME=user_profiles1
```

The batch size, try to keep it small, because growing to large can hurt performance negatively. 500 to 1000 works well. Parallel threads should follow 2x of your available CPU, for a 8 CPU machine, set it to 16 and so on. The Keyspace and Table Name variables point to the source and will automatically be created on the target cluster if not already there.

## Usage

Once the python packages, virtual environment and the `.env` setup is done, execute the `migrate.py` using 

```
❯ python migrate.py
Creating target keyspace my_keyspace
Creating target table user_profiles
Data Migration: 1000000rows [00:53, 18626.43rows/s]
```

The script will generate a simple output with progress marker, rows per second and duration. 

### Thank you.
