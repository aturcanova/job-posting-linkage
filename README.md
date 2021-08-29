# Job Posting Linkage

This is an Interdisciplinary Project (IDP) from TUM Informatics and TUM School of Management (Chair of Financial Management and Capital Markets).


## Description


Novel datasets often lack common identifiers used by commercial databases. 
A well-established approach is to use company names and other firm characteristics to apply linking methods.
This project aims to link two datasets on German companies using the python package RecordLinkage. 
In particular, our goal is to link a large-scale dataset on firms' job postings to a ready-made standard dataset on firm financials using this package.
The outcome of the project is an optimal crosswalk between the two datasets. 

We summarize our work as follows:
* We preprocess both datasets in a similar way, applying standardization and cleaning techniques. In order to retrieve rudimentary information, we replace or remove redundant words and characters. 
* We use the Python Record Linkage Toolkit to link the datasets on multiple attributes, exploiting several indexing algorithms and similarity measures.
* Finally, we outline shortcomings of the chosen approach and possible future enhancements.



## Getting Started

### Dependencies

* Python>=3.6

```
pip install -r requirements.txt 
```

### Installing

```
pip --no-cache-dir install . --user
```

### Running

Jupyter notebooks are situated in the folder _/notebooks_.

## Authors

* **Andrea Turčanová**


## Acknowledgments

### Advisor

* Lisa Knauer, M.Sc.

### External data sources

#### German ZIP codes

* German-Zip-Codes

* Author: Jan Brennenstuhl

* https://gist.github.com/jbspeakr/4565964

#### Dictionary of 40k most common German words

* DEREWO (2007): Korpusbasierte Wortgrundformenliste DEREWO, derewo-v-40000g-2009-12-31-0.1,mit Benutzerdokumentation

* Author: Institut für Deutsche Sprache, Programmbereich Korpuslinguistik

* http://www.ids-mannheim.de/kl/derewo/