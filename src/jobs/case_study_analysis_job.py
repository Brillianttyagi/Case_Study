import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from src.constants import constants
from src.utils.file_utils import read_csv


class CaseStudyAnalysis:
    """
    A class that performs various analysis on case study data.

    Attributes:
    - spark (SparkSession): The SparkSession object used for data processing.
    - config (dict): A dictionary containing configuration parameters.

    Methods:
    - __init__(self, spark, config): Initializes the CaseStudyAnalysis object.
    - count_crashes_where_males_killed(self) -> int: Counts the number of crashes where
                                                     males were killed.
    - count_two_wheelers_crashes(self) -> int: Counts the number of crashes involving two-wheelers.
    - analyse_fatal_crashes(self) -> DataFrame: Analyzes the top 5 vehicle makes of cars involved
                                                in fatal crashes.
    - count_vehicles_with_special_conditions(self) -> int: Counts the number of vehicles with
                                                          drivers having valid licenses involved
                                                          in hit and run accidents.
    - count_drivers_by_license_state(self) -> DataFrame: Determines the state with the highest
                                                         number of accidents where females are
                                                         not involved.
    - analyse_injuries_by_vehicle_make(self) -> DataFrame: Analyzes the top 3rd to 5th vehicle makes
                                                           that contribute to the largest number of
                                                           injuries including death.
    - clean_and_aggregate_data(self) -> DataFrame: Cleans and aggregates data to show unique
                                                   combinations of vehicle body style and ethnicity.
    - analyse_alcohol_contributions(self) -> DataFrame: Analyzes the top 5 ZIP codes with the
                                                        highest number of crashes involving alcohol.
    - filter_units_and_damages(self) -> DataFrame: Filters data to show the count of distinct
                                                   crash IDs where no damaged property was
                                                   observed and damage level is above 4 and car has
                                                   valid insurance.
    """

    def __init__(self, spark, config):
        self.changes_use = read_csv(spark, config.get("CHANGES_USE"))
        self.damages_use = read_csv(spark, config.get("DAMAGES_USE"))
        self.endorse_use = read_csv(spark, config.get("ENDORSE_USE"))
        self.primary_person_use = read_csv(spark, config.get("PRIMARY_PERSON_USE"))
        self.restrict_use = read_csv(spark, config.get("RESTRICT_USE"))
        self.units_use = read_csv(spark, config.get("UNITS_USE"))

    def count_crashes_where_males_killed(self) -> int:
        """
        The number of crashes (accidents) in which number of males killed are
        greater than 2

        Parameters:
        self: An instance of the class.

        Returns:
        int: The count of crashes meeting the specified criteria.
        """
        # Perform the analysis and return the result
        return self.primary_person_use.filter(
            (F.col(constants.DEATH_CNT) > 2) & (F.col(constants.PRSN_GNDR_ID) == "MALE")
        ).count()

    def count_two_wheelers_crashes(self) -> int:
        """
        Count the number of crashes involving two-wheelers (e.g., motorcycles).

        Parameters:
        self: An instance of the class.

        Returns:
        int: The count of crashes involving two-wheelers.
        """
        # Perform the analysis and return the result
        return self.units_use.filter(
            F.col(constants.VEH_BODY_STYL_ID).contains("MOTORCYCLE")
        ).count()

    def analyse_fatal_crashes(self) -> DataFrame:
        """
        Analyse the top 5 vehicle makes of the cars present in the crashes in
        which the driver died and airbags did not deploy.

        Parameters:
        self: An instance of the class.

        Returns:
        DataFrame: The result of the analysis, showing the top 5 vehicle makes
                    involved in fatal crashes.
        """
        # Filter units_use DataFrame
        units_use_filtered = self.units_use.filter(F.col(constants.VEH_MAKE_ID) != "NA")

        # Filter primary_person_use DataFrame
        primary_person_use_filtered = self.primary_person_use.filter(
            (F.col(constants.PRSN_TYPE_ID) == "DRIVER")
            & (F.col(constants.PRSN_INJRY_SEV_ID) == "KILLED")
            & (F.col(constants.PRSN_AIRBAG_ID) == "NOT DEPLOYED")
        )

        # Perform the join and analysis
        return (
            units_use_filtered.join(
                primary_person_use_filtered, constants.CRASH_ID, "inner"
            )
            .groupBy(constants.VEH_MAKE_ID)
            .count()
            .orderBy(F.col("count").desc())
            .limit(5)
            .select(constants.VEH_MAKE_ID)
        )

    def count_vehicles_with_special_conditions(self) -> int:
        """
        Count the number of vehicles with drivers having valid licenses involved
        in hit and run accidents.

        Parameters:
        self: An instance of the class.

        Returns:
        int: The count of vehicles with special conditions.
        """
        # Filter primary_person_use DataFrame
        primary_person_use_filtered = self.primary_person_use.select(
            constants.CRASH_ID, constants.DRVR_LIC_TYPE_ID
        ).filter(
            F.col(constants.DRVR_LIC_TYPE_ID).isin(
                "COMMERCIAL DRIVER LIC.", "DRIVER LICENSE"
            )
        )

        # Filter units_use DataFrame
        units_use_filtered = self.units_use.select(
            constants.CRASH_ID, constants.VEH_HNR_FL
        ).filter(F.col(constants.VEH_HNR_FL) == "Y")

        # Perform the join and count
        return primary_person_use_filtered.join(
            units_use_filtered, constants.CRASH_ID, "inner"
        ).count()

    def count_drivers_by_license_state(self) -> DataFrame:
        """
        State has highest number of accidents in which females are not involved.

        Parameters:
        self: An instance of the class.

        Returns:
        DataFrame: First state name.
        """
        # Perform the analysis and return the result
        return (
            self.primary_person_use.filter(F.col(constants.PRSN_GNDR_ID) != "FEMALE")
            .groupBy(constants.DRVR_LIC_STATE_ID)
            .count()
            .orderBy(F.col("count").desc())
            .select(constants.DRVR_LIC_STATE_ID)
            .first()
        )

    def analyse_injuries_by_vehicle_make(self) -> DataFrame:
        """
        the Top 3rd to 5th VEH_MAKE_IDs that contribute to a
        largest number of injuries including death.

        Parameters:
        self: An instance of the class.

        Returns:
        DataFrame: The result of the analysis, showing total injuries by vehicle make.
        """
        # Create a new column for total injuries including death
        total_injury_df = self.primary_person_use.withColumn(
            "TOT_INJRY_INCLUDING_DEATH",
            F.col(constants.TOT_INJRY_CNT) + F.col(constants.DEATH_CNT),
        )

        # Join primary_person_use and units_use DataFrames on "CRASH_ID"
        joined_df = total_injury_df.join(self.units_use, constants.CRASH_ID, "inner")

        # Group by "VEH_MAKE_ID" and aggregate the sum of total injuries including death
        result_df = (
            joined_df.groupBy(constants.VEH_MAKE_ID)
            .agg(sum(F.col("TOT_INJRY_INCLUDING_DEATH")).alias("TOTAL_INJRY"))
            .orderBy(F.col("TOTAL_INJRY").desc())
            .select(constants.VEH_MAKE_ID)
        )

        # Return the result DataFrame with the top 3 to 5 vehicle makes
        return result_df.limit(5).subtract(result_df.limit(2))

    def clean_and_aggregate_data(self) -> DataFrame:
        """
        All the body styles involved in crashes, mention the top
        ethnic user group of each unique body style

        Parameters:
        self: An instance of the class.

        Returns:
        DataFrame: The result of the aggregation, showing unique combinations of
                   vehicle body style and ethnicity.
        """
        # Clean primary_person_use DataFrame
        primary_person_use_filtered = self.primary_person_use.filter(
            ~F.col(constants.PRSN_ETHNICITY_ID).isin(["NA", "UNKNOWN"])
        )

        # Define values to remove from units_use DataFrame
        values_to_remove = [
            "NA",
            "UNKNOWN",
            "NOT REPORTED",
            "OTHER  (EXPLAIN IN NARRATIVE)",
        ]

        # Clean units_use DataFrame
        units_use_filtered = self.units_use.filter(
            ~F.col(constants.VEH_BODY_STYL_ID).isin(values_to_remove)
        )

        # Define a window specification for ranking by count in descending order
        window_spec = Window.partitionBy(constants.VEH_BODY_STYL_ID).orderBy(
            F.desc("count")
        )

        # Join and aggregate data
        return (
            primary_person_use_filtered.join(
                units_use_filtered, constants.CRASH_ID, "inner"
            )
            .groupBy(constants.VEH_BODY_STYL_ID, constants.PRSN_ETHNICITY_ID)
            .agg(F.count("*").alias("count"))
            .withColumn("row", F.row_number().over(window_spec))
            .filter(F.col("row") == 1)
            .drop("row", "count")
        )

    def analyse_alcohol_contributions(self) -> DataFrame:
        """
        The Top 5 Zip Codes with highest number crashes with
        alcohols as the contributing factor to a crash.

        Parameters:
        self: An instance of the class.

        Returns:
        DataFrame: The result of the analysis, showing the top 5 ZIP codes with
                   alcohol-related contributions.
        """
        # Perform the analysis and return the result
        return (
            self.primary_person_use.dropna(subset=[constants.DRVR_ZIP])
            .join(self.units_use, constants.CRASH_ID, "inner")
            .filter(
                (F.col(constants.CONTRIB_FACTR_1_ID).contains("ALCOHOL"))
                | (F.col(constants.CONTRIB_FACTR_2_ID).contains("ALCOHOL"))
            )
            .groupBy(constants.DRVR_ZIP)
            .agg(F.count("*").alias("count"))
            .orderBy(F.col("count").desc())
            .limit(5)
        )

    def filter_units_and_damages(self) -> DataFrame:
        """
        Count of Distinct Crash IDs where No Damaged Property was observed
        and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance.

        Parameters:
        self: An instance of the class.

        Returns:
        DataFrame: The result of filtering, showing the CRASH_ID column.

        Assumptions:
        The following are considered valid insurance types for vehicles:

        [
            "INSURANCE BINDER",
            "LIABILITY INSURANCE POLICY",
            "CERTIFICATE OF SELF-INSURANCE",
            "CERTIFICATE OF DEPOSIT WITH COUNTY JUDGE",
            "CERTIFICATE OF DEPOSIT WITH COMPTROLLER",
            "SURETY BOND",
            "PROOF OF LIABILITY INSURANCE"
        ]
        """
        # Filter units_use DataFrame
        units_use_filtered = self.units_use.filter(
            (
                (F.col(constants.VEH_DMAG_SCL_1_ID) > "DAMAGED 4")
                & ~F.col(constants.VEH_DMAG_SCL_1_ID).isin(
                    ["NA", "NO DAMAGE", "INVALID VALUE"]
                )
            )
            & (
                (F.col(constants.VEH_DMAG_SCL_2_ID) > "DAMAGED 4")
                & ~F.col(constants.VEH_DMAG_SCL_2_ID).isin(
                    ["NA", "NO DAMAGE", "INVALID VALUE"]
                )
            )
        ).filter(
            F.col(constants.FIN_RESP_TYPE_ID).isin(
                [
                    "INSURANCE BINDER",
                    "LIABILITY INSURANCE POLICY",
                    "CERTIFICATE OF SELF-INSURANCE",
                    "CERTIFICATE OF DEPOSIT WITH COUNTY JUDGE",
                    "CERTIFICATE OF DEPOSIT WITH COMPTROLLER",
                    "SURETY BOND",
                    "PROOF OF LIABILITY INSURANCE",
                ]
            )
        )

        # Filter damages_use DataFrame
        damage_use_filtered = self.damages_use.filter(
            F.col(constants.DAMAGED_PROPERTY) == "NONE"
        )

        # Perform the join and return the result
        return units_use_filtered.join(
            damage_use_filtered, constants.CRASH_ID, "inner"
        ).select(constants.CRASH_ID)


def analyse_units_charges(self) -> DataFrame:
    """
    The Top 5 Vehicle Makes where drivers are charged with
    speeding related offences, has licensed Drivers, used
    top 10 used vehicle colours and has car licensed with
    the Top 25 states with highest number
    of offences.

    Parameters:
    self: An instance of the class.

    Returns:
    DataFrame: The result of the analysis, showing the top 5 vehicle
               makes with charges related to speeding.
    """
    # Find the top 10 vehicle colors
    top_10_vehicle_color = (
        self.units_use.filter(F.col(constants.VEH_COLOR_ID) != "NA")
        .groupBy(constants.VEH_COLOR_ID)
        .count()
        .orderBy(F.col("count").desc())
        .select(constants.VEH_COLOR_ID)
        .limit(10)
        .collect()
    )
    top_10_vehicle_color_list = [row[0] for row in top_10_vehicle_color]

    # Find the top 25 states with the highest number of offenses
    # filter is done because VEH_LIC_STATE_ID contains some integer values
    top_25_states_highest_nmbr_offences = (
        self.units_use.filter(F.col(constants.VEH_LIC_STATE_ID).cast("int").isNull())
        .groupBy(constants.VEH_LIC_STATE_ID)
        .count()
        .orderBy(F.col(constants.VEH_LIC_STATE_ID).desc())
        .limit(25)
        .collect()
    )
    top_25_states_highest_nmbr_offences_list = [
        row[0] for row in top_25_states_highest_nmbr_offences
    ]

    # Join units_use, charges_use, and primary_person_use DataFrames
    return (
        self.units_use.join(self.charges_use, constants.CRASH_ID, "inner")
        .join(self.primary_person_use, constants.CRASH_ID, "inner")
        .filter(F.col(constants.CHARGE).contains("SPEED"))
        .filter(
            F.col(constants.DRVR_LIC_TYPE_ID).isin(
                "COMMERCIAL DRIVER LIC.", "DRIVER LICENSE"
            )
        )
        .filter(
            F.col(constants.VEH_COLOR_ID).isin(top_10_vehicle_color_list)
            & F.col(constants.VEH_LIC_STATE_ID).isin(
                top_25_states_highest_nmbr_offences_list
            )
        )
        .groupBy(constants.VEH_MAKE_ID)
        .count()
        .orderBy(F.col("count").desc())
        .select(constants.VEH_MAKE_ID)
        .limit(5)
    )
