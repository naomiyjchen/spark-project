
# Environment Setup


## Create the virtual environment

```
python -m venv .venv
source .venv/bin/activate

pip install pyspark torch transformers huggingface_hub venv-pack
```


Package the environment into a tar.gz archive:

```
(.venv) $ venv-pack -o environment.tar.gz
Collecting packages...
Packing environment at 
```



## Start Spark Shell

To launch the interactive job, do the following within the virtual environment,
```
PYSPARK_DRIVER_PYTHON=`which python` \
PYSPARK_PYTHON=./environment/bin/python \
pyspark \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
--master yarn \
--deploy-mode client \
--archives environment.tar.gz#environment

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
