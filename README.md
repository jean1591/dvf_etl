# DVF ETL
Perform ETL from [dvf](https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/).

This script will extract data, transform and load it to custom DB.


## Usage
To run script with data from 2019:
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

#### Treatment
- Load csv file to DataFrame
- Drop rows if:
  - `typeOfSearch` or `price` are `NaN`
  - `typeOfBuilding`, `surface` and `nbRoom` are `NaN` 
- Update fields values:
  - `typeOfBuilding` to lowercase
  - `typeOfSearch` to lowercase
  - `Vente` is updated to `achat`
- Drop useless rows:
  - `typeOfBuilding` must be either `appartement` or `maison`
- Validation:
  - `price` must be bounds to 4 999 and 1 999 999
  - `surface` must be bounds to 10 and 1 000
  - `typeOfSearch` is limited to `achat`
- Groupby: Group per `_idMutation` and sum `surface` and `nbRoom` together (`price` being already summed up). For all other rows, the first element is retrieved and displayed


### Load
To do

## Python dependancies
Dependancies | Purpose | Install
--- | --- | ---
argparse | Fetch args passed to script | -
pandas | Transform data | `pip install pandas`
numpy | Maths logic | `pip install numpy`
progress | Show progess bar | `pip install progress`