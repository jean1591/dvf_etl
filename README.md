# DVF ETL
Perform ETL from [dvf](https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/).

This script will extract data, transform and load it to custom DB.


## Usage
To extract data from `2019`, transform and load it to DB named `db_name` and collection name `c_name`:   
`sh main.sh -y 2019 -d db_name -c c_name`

**Note:**
Minimum year value is 2014, maximum is 2019.

### Example
Running `sh main.sh -y 2017 -d perso -c DVF` should display: 
```bash
>> PERFORMING ETL ON DVF_2017 <<

>> EXTRACT
Downloading https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/2017/full.csv.gz
data/dvf_2017.csv.gz      100%[==================================>]  90,34M  51,7MB/s    ds 1,7s
Extracting dvf_2017.csv.gz into dvf_2017.csv

>> TRANSFORM
Processing Data           |██████████████████████████████████████████████████| 6/6
Execution time: 0:00:24

>> LOAD
Loading dvf_2017_updated.csv into perso.DVF
2020-09-10T10:14:20.871+0200	connected to: mongodb://localhost/
2020-09-10T10:14:20.872+0200	dropping: perso.DVF
2020-09-10T10:14:23.873+0200	[#####...................] perso.DVF	23.3MB/101MB (23.0%)
2020-09-10T10:14:26.875+0200	[###########.............] perso.DVF	49.5MB/101MB (48.8%)
2020-09-10T10:14:29.872+0200	[#################.......] perso.DVF	74.5MB/101MB (73.5%)
2020-09-10T10:14:32.874+0200	[#######################.] perso.DVF	99.5MB/101MB (98.1%)
2020-09-10T10:14:33.112+0200	[########################] perso.DVF	101MB/101MB (100.0%)
2020-09-10T10:14:33.112+0200	897213 document(s) imported successfully. 0 document(s) failed to import.

>> EOF <<
```


## Details
### Extract
Done with bash.   
Download data from [dvf](https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/) as `filename.csv.gz`, extract data into `filename.csv` and delete downloaded file.

### Transform
Done with Python & pandas.   
Load extracted file into pandas DataFrame and perform following treatments:

- Load csv file to DataFrame
- Drop rows if:
  - either `typeOfSearch` or `price` are `NaN`
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
Done with bash.   
Load updated CSV file to MongoDB using `mongoimport`.


## Python dependancies
Dependancies | Purpose | Install
--- | --- | ---
argparse | Fetch args passed to script | -
pandas | Transform data | `pip install pandas`
numpy | Maths logic | `pip install numpy`
progress | Show progess bar | `pip install progress`