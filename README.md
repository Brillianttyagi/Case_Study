# Case_Study

## Project Description

Data Set folder has 6 csv files.

1. Charges_use.csv
2. Damages_use.csv
3. Endorse_use.csv
4. Primary_Person_use.csv
5. Restrict_use.csv
6. Units_use.csv

Here is the schema for the data:
![Schema](schema/schema.png)

## Project Questions

1. Find the number of crashes (accidents) in which the number of males killed is greater than 2.

2. How many two-wheelers are booked for crashes?

3. Determine the Top 5 Vehicle Makes of the cars present in the crashes in which the driver died and Airbags did not deploy.

4. Determine the number of Vehicles with drivers having valid licenses involved in hit and run.

5. Which state has the highest number of accidents in which females are not involved?

6. Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to the largest number of injuries, including death.

7. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style.

8. Among the crashed cars, what are the Top 5 Zip Codes with the highest number of crashes with alcohol as the contributing factor to a crash (Use Driver Zip Code).

9. Count the Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and the car has insurance.

10. Determine the Top 5 Vehicle Makes where drivers are charged with speeding-related offenses, have licensed drivers, used the top 10 used vehicle colors, and have cars licensed with the Top 25 states with the highest number of offenses (to be deduced from the data).

## Folder Structure

```
├───config
├───data
├───docs
├───notebooks
├───schema
├───scripts
├───src
│   ├───constants
│   ├───jobs
│   └───utils
└───tests
```

## How to run the project

1. **Clone the repository**
2. **Open the terminal and navigate to the project folder**
3. **Create a virtual environment and activate it**
   ```bash
   virtualenv venv
    source venv/bin/activate
   ```
4. **Install the dependencies**
   ```bash
   pip install -r requirements.txt
   ```
5. **Unzip or move all data files in the data folder**
6. **Run the following command:**
   ```bash
   make build  # To build the egg file
   make run    # To run the Spark job
   make help   # To display the available targets and their descriptions
   ```
7. **After build you can also run this analysis using spark-submit**
   ```bash
   spark-submit --master "local[*]" --py-files dist/BCG_Analysis-0.0.1-py3.10.egg main.py
   ```
8. **The results will be saved in the output folder**

## Author

[Deepanshu Tyagi](https://github.com/Brillianttyagi)

## Assumptions of output format

I have assumed that the output of the analysis will be saved in the output folder in the form of csv file.

```bash
| Analysis No | Result                                               |
|-------------|------------------------------------------------------|
| Analysis 1  | 0                                                    |
| Analysis 2  | 784                                                |
| Analysis 3  | CHEVROLET, FORD, DODGE, FREIGHTLINER, NISSAN          |
| Analysis 4  | 6359                                               |
| Analysis 5  | Texas                                              |
| Analysis 6  | TOYOTA, DODGE, NISSAN                               |
| Analysis 7  | AMBULANCE: WHITE, BUS: HISPANIC, FARM EQUIPMENT: WHITE, FIRE TRUCK: WHITE, MOTORCYCLE: WHITE, NEV-NEIGHBORHOOD ELECTRIC VEHICLE: WHITE, PASSENGER CAR, 2-DOOR: WHITE, PASSENGER CAR, 4-DOOR: WHITE, PICKUP: WHITE, POLICE CAR/TRUCK: WHITE, POLICE MOTORCYCLE: HISPANIC, SPORT UTILITY VEHICLE: WHITE, TRUCK: WHITE, TRUCK TRACTOR: WHITE, VAN: WHITE, YELLOW SCHOOL BUS: WHITE |
| Analysis 8  | 76010, 78521, 75067, 78574, 75052                   |
| Analysis 9  |                                                      |
| Analysis 10 | FORD, CHEVROLET, TOYOTA, DODGE, NISSAN                |

```

**Note: We can save output in any format as per the requirement.**

## Assumptions for the analysis

- In Analysis 9 I have assumed these car insurance are valid insurances.

```python
        [
            "INSURANCE BINDER",
            "LIABILITY INSURANCE POLICY",
            "CERTIFICATE OF SELF-INSURANCE",
            "CERTIFICATE OF DEPOSIT WITH COUNTY JUDGE",
            "CERTIFICATE OF DEPOSIT WITH COMPTROLLER",
            "SURETY BOND",
            "PROOF OF LIABILITY INSURANCE"
        ]
```

- In Analysis 9 Damage Level (VEH_DMAG_SCL~) is above 4 for VEH_DMAG_SCL_1_ID and VEH_DMAG_SCL_2_ID.


If you have any questions or suggestions, feel free to open an issue or contact me on [Deepanshu Tyagi](https://github.com/Brillianttyagi).

## Future Scope

- We can also add seprate utils for running this on gcp dataproc as well as on aws emr.
- We can also add the test cases for the spark jobs.
- We can also add cloud reading and writing capabilities to the spark jobs.
- We can also add the more logging and monitoring to the spark jobs.
