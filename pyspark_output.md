## Operation: Load Data

### Output:

|    | Series_reference   | Period     | Type           |   Data_value |   Lower_CI |   Upper_CI | Units    | Indicator   | Cause   | Validation   | Population   | Age      | Severity   |
|---:|:-------------------|:-----------|:---------------|-------------:|-----------:|-----------:|:---------|:------------|:--------|:-------------|:-------------|:---------|:-----------|
|  0 | W_A11              | 2000-02-01 | Moving average |      59.6667 |    50.9258 |    68.4075 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |
|  1 | W_A11              | 2001-03-01 | Moving average |      60      |    51.2348 |    68.7652 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |
|  2 | W_A11              | 2002-04-01 | Moving average |      59      |    50.3081 |    67.6919 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |
|  3 | W_A11              | 2003-05-01 | Moving average |      59      |    50.3081 |    67.6919 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |
|  4 | W_A11              | 2004-06-01 | Moving average |      61.3333 |    52.4713 |    70.1954 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |
|  5 | W_A11              | 2005-07-01 | Moving average |      63      |    54.0183 |    71.9817 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |
|  6 | W_A11              | 2006-08-01 | Moving average |      57.3333 |    48.7651 |    65.9016 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |
|  7 | W_A11              | 2007-09-01 | Moving average |      63.6667 |    54.6376 |    72.6957 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |
|  8 | W_A11              | 2008-10-01 | Moving average |      64      |    54.9473 |    73.0527 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |
|  9 | W_A11              | 2009-11-01 | Moving average |      64.3333 |    55.2571 |    73.4096 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |

---

## Operation: Spark SQL Query

### Query:

```sql

    SELECT Severity, COUNT(*) AS total_cases
    FROM injuryOutcome
    GROUP BY Severity
    ORDER BY total_cases DESC
    
```

### Output:

|    | Severity          |   total_cases |
|---:|:------------------|--------------:|
|  0 | Serious           |          1062 |
|  1 | Fatal             |           944 |
|  2 | Serious non-fatal |           942 |

---

## Operation: Data Transformation

### Output:

|    | Series_reference   | Period     | Type           |   Data_value |   Lower_CI |   Upper_CI | Units    | Indicator   | Cause   | Validation   | Population   | Age      | Severity   |   year |   before_2010 |
|---:|:-------------------|:-----------|:---------------|-------------:|-----------:|-----------:|:---------|:------------|:--------|:-------------|:-------------|:---------|:-----------|-------:|--------------:|
|  0 | W_A11              | 2000-02-01 | Moving average |      59.6667 |    50.9258 |    68.4075 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |   2000 |             1 |
|  1 | W_A11              | 2001-03-01 | Moving average |      60      |    51.2348 |    68.7652 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |   2001 |             1 |
|  2 | W_A11              | 2002-04-01 | Moving average |      59      |    50.3081 |    67.6919 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |   2002 |             1 |
|  3 | W_A11              | 2003-05-01 | Moving average |      59      |    50.3081 |    67.6919 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |   2003 |             1 |
|  4 | W_A11              | 2004-06-01 | Moving average |      61.3333 |    52.4713 |    70.1954 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |   2004 |             1 |
|  5 | W_A11              | 2005-07-01 | Moving average |      63      |    54.0183 |    71.9817 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |   2005 |             1 |
|  6 | W_A11              | 2006-08-01 | Moving average |      57.3333 |    48.7651 |    65.9016 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |   2006 |             1 |
|  7 | W_A11              | 2007-09-01 | Moving average |      63.6667 |    54.6376 |    72.6957 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |   2007 |             1 |
|  8 | W_A11              | 2008-10-01 | Moving average |      64      |    54.9473 |    73.0527 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |   2008 |             1 |
|  9 | W_A11              | 2009-11-01 | Moving average |      64.3333 |    55.2571 |    73.4096 | Injuries | Number      | Assault | Validated    | Whole pop    | All ages | Fatal      |   2009 |             1 |

---

