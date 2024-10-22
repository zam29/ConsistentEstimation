# Experiments files :
### - full relations :
   - real_missing_experiments.py
   - injected_missing_expermints.py
   
### - stopping early :
   - mar_stoppingEarly.py
   - mcar_stoppingEarly.py

## To run the experiments:
Choose the desired experiment file and type :
`python3 real_missing_experiments.py`

# Individual query testing:
- main.py

To test specific query, follow the commented example in the `main.py` file 
the `Multi_UI.py` file  has all the flags in its constructor. Pick the desired flag to test within main.py. To run:

`python3 main.py`
# Model-Based related files :
- codeTests/codeAssembler.py
- aggregateFunctions.py
- DistributionDriven_Join.py
- FullJointDistEstimation.py
- Multi_GraphH.py
- Multi_UI.py
- sampling.py
- Selection_MAR.py
- StateMachine.py

# Sample-Based related files :
- efficient_MAR.py
- efficient_MCAR.py
- Multi_UI.py


# Interval-Based related files :
- Multi_GraphH.py
- Multi_UI.py
- IntervalAnswers.py


###  Helper file to close unterminated quotes in a dataset
- fix_quotes.py

### Single relations MCAR and MAR datasets folder :
`rwDatasets`

### MCAR relation joins datasets folder :
`joinsData`

### MAR relation joins datasets folder :
`MarJoinsData`

#### Notes:
The`data_processing`folder will hold the observed distribution file from the modeling steps for the model-based approach
