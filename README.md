# DVF ETL
Perform ETL from [dvf](https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/).

This script will extract data, transform and load it to custom DB.


## Usage
To download data from 2019:
`sh main.sh -y 2019`

**Note:**
Minimum value year is 2014, maximum is 2019.


## Details
### Extract
Done with bash.
Download data from [dvf](https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/) as `abc.csv.gz` file, and extract data and delete downloaded file.

### Transform
Done with Python & pandas.
Load extracted file into Python with pandas and perform transform on file.

### Load
To do

## Python dependancies
Dependancies | Purpose | Install
--- | --- | ---
argparse | Fetch args passed to script | -
pandas | Transform data | `pip install pandas`
numpy | Maths logic | `pip install numpy`
progress | Show progess bar | `pip install progress`