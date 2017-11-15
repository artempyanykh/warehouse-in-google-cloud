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
More formally, we need to generate a table like:

| fine_id | fine_amount | fine_registered_at | rent_id | rented_on | car_id | car_reg_number | car_make | car_model | user_id | user_name | user_passport_no | user_birth_date | user_driving_permit_since |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 15 | 2017-10-01 21:36:00 | 1 | 2017-10-01 | 3 | ks2888 | bmw | series_2 | 3 | Dumitru Matei | 482850738 | 1996-06-22 | 1999-09-21 |

We'll demonstrate how to build an ETL pipline around this problem under `local` and `cloud` modules.
Also, feel free to tune parameters in `sampledata/generate.py` to get more or less data to work with.

