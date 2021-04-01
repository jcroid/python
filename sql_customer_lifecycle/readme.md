# SQL

SQL task for postgres

## Description

Source Tables

You are given the following tables for your two tasks below:

```bash
| policy_data       |                  |                                                   |
|-------------------|------------------|---------------------------------------------------|
| policy_id         | TEXT PRIMARY KEY | ksuid assigned at purchase of   cycle             |
| user_id           | TEXT             | Unique user identifier                            |
| subscription_id   | TEXT             | KSUID that binds all cycles                       |
| policy_start_date | TIMESTAMPTZ      | Datetime the policy started                       |
| policy_end_date   | TIMESTAMPTZ      | Datetime the policy ended                         |
| underwriter       | TEXT             | Ksuid representing which underwriter has the risk |
```


Rules:

- Each subscription recurs monthly and each month gets a new policy\_id.
- A subscription can not be shared between users.

The &quot;policy\_data&quot; table provides each user&#39;s activity, listing all known details about the policy. If a user does not create a policy then where will be no row added.

**You can assume this is a large table ~500M rows.**

```bash
| finance_data           |                  |                                         |
|------------------------|------------------|-----------------------------------------|
| finance_transaction_id | TEXT PRIMARY KEY | Unique identifier to each   transaction |
| created_at             | TIMESTAMPTZ      | Time transaction was created            |
| policy_id              | TEXT             | ksuid assigned to policy                |
| reason                 | TEXT             | Reason for specific transactions        |
| premium                | INT              | Part of policy cost                     |
| ipt                    | INT              | Part of policy cost  
```

Rules:

- Every transaction must be related to a policy.
- Each policy can have multiple transactions for different reasons.

**You can assume this is a large table ~500M rows.**

There is also a calendar table that provides a base &quot;date&quot; dimension, one row per day from 2018 to 2021

```bash
| calendar     |      |            |
|--------------|------|------------|
| date         | DATE | 2020-08-02 |
| year         | INT  | 2020       |
| month_number | INT  | 8          |
| month_name   | TEXT | August     |
| day_of_month | INT  | 2          |
| day_of_week  | INT  | 7          |
```


# Task 1

Given the table definitions above write a SQL query that creates a view with the below columns:

```bash
year_month,
user_id,
active_from_month,
user_lifecycle_status,
lapsed_months
```

**Active** means that the date of risk is between policy start and end dates.

Each day a user has a certain **lifecycle** type. The user&#39;s status will change on a monthly basis based on their previous and current month&#39;s activity. The statuses are:

**New** = Active for the first time

**Active** = Active if the user is on risk and not in any other life cycle.

**Churned** = Active in the prior calendar month, but not active in the current calendar month

**Lapsed** = No activity in the prior calendar month or the current calendar month

The view should display one row per user per month, starting from the month in which they first purchased a policy. This view should give their lifecycle status for that month, and if the user has lapsed, it should show a rolling count of the number of months since they were last active.

We have provided the following files (sample\_data.zip) to help set up the database:

- ddl.sql contains the DDL of calendar, finance and policy\_analysis tables

- calendar\_data.csv holds the &quot;calendar&quot; table data between 2018-2021

- policy\_analysis\_data.csv holds a minimal amount of example data for &quot;policy\_analysis&quot; table, please feel free to add more test data to policy\_analysis to cover different scenarios.

Please note any assumptions and considerations made.