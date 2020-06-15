# Anon-Optimizer
Anon-Optimizer is a tool that, given as input a config file containing queries (aka workload), dataset information and privacy policies, tries to optimize Flash anonymization algorithm in order to anonymize the dataset and preserving the data utility needed for the workload.

This is done by checking the Prec and NU Entropy of the QID contained in the workload. 

The output of the tool is the anonymized dataset and the quality of the latter.

In order to work it requires a custom implementation of HSQLDB, that can be found here: https://github.com/rripamonti/HSQLDBeditInsubria, and the ARX Java library: https://github.com/arx-deidentifier/arx.

##License:
Copyright (c) 2001-2018, The HSQL Development Group. All rights reserved.
 
 ARX (C) 2012 - 2020 Fabian Prasser and Contributors.
