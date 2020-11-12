# DVF ETL
Perform ETL from [dvf](https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/).

This script will extract data, transform and load it to custom DB.


## Usage
To extract data from `2019`, transform and load it to DB named `db_name` and collection name `c_name`:   
`python3 main.py -y 2019 -s -d perso -c DVF -r`

**Note:**
Minimum year value is 2014, maximum is 2020.

### Args
Flag | Argument | Details | required
--- | --- | ---| :---:
-y | --year | Year to Fetch| X
-d | --db | DB name| X
-c | --collection | Collection name| X
-r | --remove | Drop collection | ---
-s | --save | Save temporary JSON file | ---
-v | --verbose | Show activity | ---


### Example
Running `python3 main.py -y 2019 -d perso -c DVF -v -s` should display: 
```bash
>> PERFORMING ETL ON DVF_2019 <<

>> EXTRACT <<
Downloading https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/2019/full.csv.gz
data/dvf_2019.csv.gz      100%[==================================>]  84,83M  50,2MB/s    ds 1,7s
Extracting dvf_2019.csv.gz into dvf_2019.csv

>> TRANSFORM <<
CSV loaded to DataFrame
NaN values dropped
Price validated
Surface validated
Rows grouped per id
DataFrame saved to JSON file
Dates modified in JSON file

>> LOAD <<
2020-11-12T11:58:06.703+0100	connected to: mongodb://localhost/
2020-11-12T11:58:09.704+0100	[####....................] meridian.DVF	83.1MB/438MB (19.0%)
2020-11-12T11:58:12.705+0100	[#########...............] meridian.DVF	167MB/438MB (38.3%)
2020-11-12T11:58:15.706+0100	[#############...........] meridian.DVF	251MB/438MB (57.3%)
2020-11-12T11:58:18.708+0100	[##################......] meridian.DVF	331MB/438MB (75.7%)
2020-11-12T11:58:21.705+0100	[######################..] meridian.DVF	413MB/438MB (94.3%)
2020-11-12T11:58:22.634+0100	[########################] meridian.DVF	438MB/438MB (100.0%)
2020-11-12T11:58:22.634+0100	927753 document(s) imported successfully. 0 document(s) failed to import.
>> EOF <<
```


## Details
### Extract
Download data from [dvf](https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/) as `filename.csv.gz`, extract data into `filename.csv` and delete downloaded file.

### Transform
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
- Groupby: Group per `_idMutation` and sum `surface` and `nbRoom` together (`price` being already summed up). For all other rows, the first element is retrieved and displayed
- Update all dates to JSON date format using external loop (=> to be improved)

### Load
Load updated JSON file to MongoDB using `mongoimport`.


## Python dependancies
Dependancies | Purpose | Install
--- | --- | ---
argparse | Fetch args passed to script | -
pandas | Transform data | `pip install pandas`
numpy | Maths logic | `pip install numpy`