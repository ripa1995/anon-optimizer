# anon-optimizer
Anon-Optimizer is a tool that, given as input a config file containing queries (aka workload), dataset information and privacy policies, tries to optimize Flash anonymization algorithm in order to anonymize the dataset and preserving the data utility needed for the workload.

This is done by checking the Prec and NU Entropy of the QID contained in the workload. 

The output of the tool is the anonymized dataset and the quality of the latter.
