# Experiments files 
### - full relations :
   - real_missing_experiments.py
   - injected_missing_expermints.py
   
### - random sampling :
   - mar_stoppingEarly.py
   - mcar_stoppingEarly.py

## To run the experiments:
Choose the desired experiment file and type :
`python3 real_missing_experiments.py`

# Individual query testing
- main.py

To test a specific query, follow the commented example in the `main.py` file 
the `Multi_UI.py` file  has all the flags in its constructor. Pick the desired flag to test within main.py. To run:

`python3 main.py`
# Distribution-Centric approach files 
- codeTests/codeAssembler.py
- aggregateFunctions.py
- DistributionDriven_Join.py
- FullJointDistEstimation.py
- Multi_GraphH.py
- Multi_UI.py
- sampling.py
- Selection_MAR.py
- StateMachine.py

# Data-Centric approach files 
- efficient_MAR.py
- efficient_MCAR.py
- Multi_UI.py


# Interval-Based approach files 
- Multi_GraphH.py
- Multi_UI.py
- IntervalAnswers.py


###  Helper file to close unterminated quotes in a dataset:
- fix_quotes.py

### Single relations MCAR and MAR datasets folder :
`rwDatasets`

### MCAR relation joins datasets folder :
`joinsData`

### MAR relation joins datasets folder :
`MarJoinsData`

#### Notes:
- The`data_processing`folder will hold the observed distribution files from the modeling steps for the model-based approach.
- You should run the code within the `ConsistentEstimators` folder :

        cd ConsistentEstimators
        python3 real_missing_experiments.py
- Once run, a folder for the model-based approach, called` ModelBasedLog`, will be created to hold the modeling and execution step times separately. We also report the model-based total time within the outputs folders listed below. 

#### Output folders:
Once run, these folders will be created and have the recored times and estimated 
-  full relations experiments : ` Injected `and  `RealWorld` folders 
-  stop early experiments :  `Mcar_StoppingEarly `and `Mar_StoppingEarly folder`
