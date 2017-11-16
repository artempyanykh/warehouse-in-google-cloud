# Building a Data Warehouse in Google Cloud. DIY

## General overview

Have you ever wondered how to level up your data processing game?
If you're transitioning from ad-hoc analytics and researching options, this might be a good starting point.

This project has two main modules:

* `local` which shows how to setup a simple data processing pipeline using Luigi, Python, Pandas and Postgres in no time.
  Though simple, this approach can get you pretty far.
* `cloud` which illustrates how you can easily swap out:
    1. local storage in favour of durable and distributed **Google Cloud Storage**,
    2. local processing power in favour of scalable **Google Dataflow**,
    3. local PostgreSQL database that you need to manage in favour of **BigQuery** which has a familiar SQL interface, but can process TBs of data without breaking a sweat and integrates nicely with GSuite accounts.

There is another module `sampledata` which is used to generate sample data.
To make it a bit more interesting imagine that the data is from a car renting company called **DailyCar**.
Specifically, we have the following information (under `sampledata/generated/`):

1. `users.csv` has information about registered clients of **DailyCar**.
2. `cars.csv` has information about its car park.
3. `rents.csv` contains a list of rents, specifically, who and when rented what car.
4. `fines.csv` is pulled from police database, and help us see all the fines (like speed limit) that are related to company's cars.

Business would like to enrich information about fines, so it's able to understand who was driving a specific car at a particular point in time.
More formally, we need to generate a table with the following fields (transposed):

| column | data |
| --- | --- |
| fine_id | 1 |
| fine_amount | 15 |
| fine_registered_at | 2017-10-01 21:36:00 |
| rent_id | 1 |
| rented_on | 2017-10-01 |
| car_id  | 3 |
| car_reg_number | ks2888 |
| car_make | bmw |
| car_model | series_2 |
| user_id | 3 |
| user_name | Dumitru Matei |
| user_passport_no | 482850738 |
| user_birth_date | 1966-06-22 |
| user_driving_permit_since | 1991-10-18 |

We'll demonstrate how to build an ETL pipline around this problem under `local` and `cloud` modules.
Also, feel free to tune parameters in `sampledata/generate.py` to get more or less data to work with.

## Setup

First, make sure you have `python 2.7`.
Then, inside project's root folder execute the following commands to install required packages:

```bash
$ pip install pipenv
$ pipenv install --skip-lock
```


For the `local` part you need to install **PostgreSQL** and create a database and a user, like this:

```bash
> psql postgres
=# create role dwh login password 'dwh';
=# create database warehouse_in_google_cloud owner dwh;
```

For the `cloud` part you need to obtain Google Cloud Service credentials and put them under `config/credentials.json`.
Don't forget to update `config/config.ini` accordingly.

## Run ETL

To run an ETL task use the following command:

```bash
$ ./run-luigi.py --local-scheduler --module=MODULE_NAME TASK_NAME --on=DATE
```

Replace `TASK_NAME` with the name of a defined task, like `ProcessFines`.
`DATE` parameter can take any value (for our purposes it doesn't matter much what value), for instance `2017-11-16`.
`MODULE_NAME` can be either `local` or `cloud`.

For example:

```bash
$ ./run-luigi.py --local-scheduler --module=cloud ProcessFines --on=2017-11-16
```

If you want to go really wild, change `runner` parameter in `config.ini` to `DataflowRunner` and unleash the full power of the cloud, as it will run Apache Beam tasks using **Google Dataflow**.

## Explore contents in Google Cloud Storage

After you run a `cloud` ETL, you may want to see the result.

If you have a Google Cloud account and your own credentials, feel free to go to the web console.
Otherwise, obtain workshop host's credentials and use a `./shell.py` script to load an iPython session with some predefined functions, such as `gls` and `gcat`.
An example usage is below:

```python
In [5]: gls('2017-11-15')
Out[5]:
[<Blob: warehouse-in-gcs-store, 2017-11-15/cars.csv>,
 <Blob: warehouse-in-gcs-store, 2017-11-15/fines.csv>,
 <Blob: warehouse-in-gcs-store, 2017-11-15/rents.csv>,
 <Blob: warehouse-in-gcs-store, 2017-11-15/rich_fines/_SUCCESS>,
 <Blob: warehouse-in-gcs-store, 2017-11-15/rich_fines/data.csv-00000-of-00001>,
 <Blob: warehouse-in-gcs-store, 2017-11-15/users.csv>]

In [6]: gcat('2017-11-15/cars.csv')
id,make,model,reg_number
1,nissan,murano,ko2116
2,hyundai,solaris,ct8988
3,bmw,series_2,ks2888


In [7]: gcat('2017-11-15/rich_fines/data.csv-00000-of-00001')
fine_id,fine_amount,fine_registered_at,rent_id,rented_on,car_id,car_reg_number,car_make,car_model,user_id,user_name,user_passport_no,user_birth_date,user_driving_permit_since
8,1,2017-10-03 09:09:00,7,2017-10-03,1,ko2116,nissan,murano,1,Cristina Ciobanu,547345952,1988-02-17,1991-02-27
...
```

## Exercises
Practice makes perfect, so if you'd like to go a little bit deeper, here are some ideas to try:

1. Task `local.LoadRichFines` will not replace contents of the table, which may not be desirable especially if you run your ETL several times a day.
   Try to implement a task that inherits from `luigi.contrib.postgres.CopyToTable`, and disregards whether it was run before or not.
2. Similarly, `cloud.LoadRichFines` wont't replace a table in BigQuery. Try to fix this.
3. There's a bit of a boilerplate in `cloud.ProcessFines` with `Map`s and `CoGroupBy`s.
   Try to implement a custom `Join` transform that does SQL-style join on two `PCollection`s.
   Example usage is:

   ```python
   ((rich_rents, fines)
            | Join(
                left_on=lambda x: (x['car_reg_number'], x['rented_on']),
                right_on=lambda x: (x['car_reg_number'], x['registered_on'])))
   ```
