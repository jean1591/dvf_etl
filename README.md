# DVF ETL
Perform ETL from [dvf](https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/).

This script will extract data, transform and load it to custom DB.


## Usage
To extract data from `2019`, transform and load it to DB named `db_name` and collection name `c_name`:   
`python3 main.py -y 2019 -s -d perso -c DVF -r`

**Note:**
Minimum year value is 2014, maximum is 2019.

### Example
Running `python3 main.py -y 2019 -d perso -c DVF -v -s` should display: 
```bash
>> PERFORMING ETL ON DVF_2019 <<

>> EXTRACT <<
Downloading https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/2015/full.csv.gz
data/dvf_2019.csv.gz      100%[==================================>]  74,30M  44,5MB/s    ds 1,7s
Extracting dvf_2019.csv.gz into dvf_2019.csv

>> TRANSFORM <<
CSV loaded to DataFrame
NaN values dropped
Price validated
Surface validated
Rows grouped per id
DataFrame saved to CSV file

>> LOAD <<
2020-11-05T17:56:01.684+0100	connected to: mongodb://localhost/
2020-11-05T17:56:04.686+0100	[######..................] perso.DVF	25.9MB/94.7MB (27.4%)
2020-11-05T17:56:07.685+0100	[#############...........] perso.DVF	52.2MB/94.7MB (55.1%)
2020-11-05T17:56:10.687+0100	[###################.....] perso.DVF	77.8MB/94.7MB (82.2%)
2020-11-05T17:56:12.643+0100	[########################] perso.DVF	94.7MB/94.7MB (100.0%)
2020-11-05T17:56:12.643+0100	818434 document(s) imported successfully. 0 document(s) failed to import.

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