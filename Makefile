APP_NAME=reddit-spoilers
NUM_EXECUTORS=16
EXECUTOR_MEMORY=20g
DRIVER_MEMORY=4g
EXECUTOR_CORES=16
PY_FILES:=$(wildcard *.py)
EGGS:=$(wildcard bin/*.egg) $(wildcard dist/*.egg)


empty:=
comma:=$(empty),$(empty)
space:=$(empty) $(empty)
PY_FILES:= $(subst $(space),$(comma),$(strip $(PY_FILES)))
EGGS:= $(subst $(space),$(comma),$(strip $(EGGS)))


BASE_COMMAND = spark-submit \
			   --name "reddit-spoilers" \
			   --py-files $(PY_FILES),$(EGGS) \
			   --num-executors $(NUM_EXECUTORS) \
			   --driver-memory $(DRIVER_MEMORY) \
			   --executor-memory $(EXECUTOR_MEMORY) \
			   --conf spark.yarn.maxAppAttempts=1

local: package
	$(BASE_COMMAND) main.py $(OPTIONS)

submit: package
	$(BASE_COMMAND) --master yarn --deploy-mode cluster --num-executors $(NUM_EXECUTORS) --executor-cores $(EXECUTOR_CORES) main.py $(OPTIONS)

package:
	python setup.py bdist_egg
