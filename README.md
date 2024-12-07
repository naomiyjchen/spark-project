
# Environment Setup


## Create the virtual environment

```
python -m venv .venv
source .venv/bin/activate

pip install pyspark torch transformers huggingface_hub

```


## Set environment variables to use the virtual environment
```
export PYSPARK_PYTHON=~/spark-project/.venv/bin/python
export PYSPARK_DRIVER_PYTHON=~/spark-project/.venv/bin/python
```

## Start Spark Shell
```
pyspark --deploy-mode client
```

You can use the below to check your environment
```
import sys
print(sys.executable)

```

## Check spark application

Check running application
```
yarn application -list
```

Kill running application, for example
```
yarn application -kill application_1724767128407_12034
```
