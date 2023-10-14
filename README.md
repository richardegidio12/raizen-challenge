# :bulb: Raizen-Challenge : ANP Fuel Sales ETL

At this repository you will find one solution to this [challenge](https://github.com/raizen-analytics/data-engineering-test/blob/master/TEST.md) from Data Engineering team at RAIZEN. 

This test consists in developing an ETL pipeline to extract internal pivot caches from consolidated reports made available by Brazilian government's regulatory agency for oil/fuels, ANP *(Agência Nacional do Petróleo, Gás Natural e Biocombustíveis)*.
<br></br>


# Context

It is asked:
- The developed pipeline is meant to extract and structure the underlying data of two of these tables:

    > -   Sales of oil derivative fuels by UF and product
    > -   Sales of diesel by UF and type

When it comes to the **xls** file, at first sight was not possible to get the datasource used to create the pivot table in the sheet. The solution adopted in this case was to use a conversion by libreoffice which allows us to get the datasource much more easily(and faster) than trying to run a solution using interaction with the excel sheet by packages available for windows. Since the resolution should be able to reproduce, the way we can do this is through docker containers, in which is simpler to create using linux base system.
<br></br>

# Resolution

Below you'll find an explain for the steps used in challenge.

### 1) Conversion of xls file
In this case we use Libreoffice to convert the file. This will allow to get the data source used to create the pivot table and make the transformations we need. At this point the new `xls` file is saved in `staging` folder.

### 2) Transformation of data
Here some stages are performed in order to get que columns needed. At this step in the end we also persist the `dataframe` in `trusted` path. 

### 3) Validating data
In order to proceed with the writing process into refined layer, a validation is performed to guarantee that the Totals obtained from volume is equal between the raw data and trusted layer.

### 4) Writing data to refined layer
After validation we can persist the data in parquet format in the final layer  
<br></br>


# Reproduce

*tip: Be sure to have docker and docker-compose isntalled at your machine.* 

The firs step is to clone this repo:
```
git clone git@github.com:richardegidio12/raizen-challenge.git 
cd raizen-challenge
```

At this point navigate to path to run Airflow through Docker:
```
cd airflow && docker-compose up -d
```

Hold on until the all service be started. As soon as they're ready, open your browser and type:
```
https://localhost:8080
```
For login use:
```
user: airflow
password: airflow
```
At the UI in Airflow, search for the DAG: `raizen-extraction-anp` and trigger it!

