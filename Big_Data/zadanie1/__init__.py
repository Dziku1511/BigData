from typing import List

from GoogleDataLoader import GoogleDataLoader
from CSVDataDAO import CSVDataDAO
import pandas as pd
from utils import removeNegative


def zadanie1() -> None:
    print("Zadanie 1")
    dataPersistence = CSVDataDAO("result", zadanie=1)
    dataLoader = GoogleDataLoader(limit=10000)

    def getTimeFrame(field: str = "") -> pd.DataFrame:
        rest: str = "" if field == "" else f"WHERE {field} IS NOT NULL AND {field} != 0"
        timeFrame: pd.DataFrame = dataLoader.get(
            f"DATE_DIFF(MAX(date), MIN(date), DAY) AS difference_days, MAX(date) as stop, MIN(date) as start",
            rest=rest)
        return timeFrame

    def countZeros(field: str = "") -> pd.Timedelta:
        field_mask: str = f"sum_{field}"
        res: pd.DataFrame = dataLoader.get(f"date, SUM({field}) AS {field_mask}", rest=f"GROUP BY date ORDER BY date")
        res[field_mask] = res[field_mask].fillna(0)
        res['time_diff'] = res['date'].diff(1).fillna(pd.to_timedelta(1, "D"))
        acc: pd.Timedelta = pd.to_timedelta(0, "D")
        for index, row in res.iterrows():
            if row[field_mask] == 0:
                acc += row['time_diff']
        return acc

    # load data from google
    rawResult: pd.DataFrame = dataLoader.getData()
    # remove duplicates and NaN values from raw data
    dataPersistence.save(rawResult, filename="initial_data")
    rawResult.drop_duplicates(inplace=True)
    rawResult.dropna(inplace=True)
    columns: List[str] = ["new_confirmed", "new_deceased", "new_persons_vaccinated"]

    # 3.1
    num_rows: pd.DataFrame = dataLoader.get("COUNT(*) as suma")
    print(f"\n3.1\nNumber of rows\n: {num_rows}")

    # 3.2
    num_countries: pd.DataFrame = dataLoader.get("COUNT(country_name) AS suma, country_name",
                                                 rest="GROUP BY country_name")
    print(f"\n3.2\nNumber of entries per country\n: {num_countries}")

    # 3.3
    # Często dla krajów niektóre kolumny liczbowe są zapisywane jako "utf-8" lub z brakującą wartością.
    # Kolumny typu (restrictions_on_gatherings,stay_at_home_requirements) są zapisywane liczbowo (jednak brak legendy utrudnia ich analizę).
    # Kolumny typu (maximum_temperature_celsius, rainfall_mm) posiadają już jednostkę w nazwię.
    # Natomiast kolumny (mobility_transit_stations, mobility_workplaces itd.) posiadają wartości dodanie i ujemne, co bez opisu utrudnia interpretację.

    # 3.4
    print("\n3.4")
    for col in ["", *columns]:
        print(f"Time difference {col}: \n{getTimeFrame(col)}")

    # 3.5
    print("\n3.5")
    for col in columns:
        print(f"Time with zero in {col}: {countZeros(col)}")

    # 3.6
    # Dane zapisywane są co dany okres, bliżej nieokreślony odstęp czasu.
    # W przypadku Stanów Zjednoczonych mamy również podział na stany. Dane są zapisywane oddzielnie dla każdego stanu.
    # Dla każdego dnia dane są zapisane (niekotniecznie dla każdego regionu/kraju)

    # 3.7
    print("\n3.7")
    cols1: List[str] = ["population_density", "new_confirmed", "nurses_per_1000", "physicians_per_1000", "new_deceased"]
    stat1: pd.DataFrame = dataLoader.get(f"country_name" + "".join(f", AVG({c}) AS avg_{c}" for c in cols1),
                                         rest=f"GROUP BY country_name")
    print(stat1)

    cols2: List[str] = [
        "restrictions_on_gatherings", "stay_at_home_requirements", "workplace_closing", "school_closing",
        "new_deceased", "new_confirmed"
    ]
    stat2: pd.DataFrame = dataLoader.get(f"country_name" + "".join(f", AVG({c}) AS avg_{c}" for c in cols2),
                                         rest=f"GROUP BY country_name")
    print(stat2)

    cols3: List[str] = [
        "new_tested_male", "new_tested_female",
        "new_deceased", "new_deceased_male", "new_deceased_female",
        "new_confirmed", "new_confirmed_male", "new_confirmed_female"
    ]
    stat3: pd.DataFrame = dataLoader.get(f"country_name" + "".join(f", SUM({c}) AS sum_{c}" for c in cols3),
                                         rest="WHERE country_name IS NOT NULL "
                                              + "".join(f"AND NOT IS_NAN({c}) " for c in cols3)
                                              + "GROUP BY country_name")
    print(stat3)

    cols4: List[str] = [
        "new_deceased", "new_confirmed", "new_recovered", "new_persons_vaccinated"
    ]
    stat4: pd.DataFrame = dataLoader.get(f"date" + "".join(f", SUM({c}) AS sum_{c}" for c in cols4),
                                         rest="WHERE date IS NOT NULL "
                                              + "".join(f"AND NOT IS_NAN({c}) " for c in cols4)
                                              + f"GROUP BY date ORDER BY date ASC")
    print(stat4)

    cols5: List[str] = ["new_confirmed", "population"]
    stat5: pd.DataFrame = dataLoader.get(
        f"testing_policy" + "".join(f", SUM({c}) AS sum_{c}" for c in cols5)
        + ", SUM(new_tested_male + new_tested_female) AS sum_new_tested",
        rest=f"GROUP BY testing_policy")
    print(stat5)

    sub = f"ROUND(average_temperature_celsius, 0) AS temp"
    cols6: List[str] = ["new_confirmed", "new_deceased", "population"]
    stat6: pd.DataFrame = dataLoader.get(
        sub + "".join(f", SUM({c}) AS sum_{c}" for c in cols6),
        rest="GROUP BY temp ORDER BY temp DESC")
    print(stat6)

    # 4.1. basic data about all countries
    dataLoader.setQueryParams(
        fields=["date", "location_key", "country_name", "population", "population_female", "population_male",
                "area_sq_km",
                "population_density", "cumulative_confirmed", "cumulative_deceased"])
    basicData: pd.DataFrame = dataLoader.getData()

    # remove duplicates and NaN values from basic data
    removeNegative(basicData)
    basicData.drop_duplicates(inplace=True)
    basicData.dropna(inplace=True)

    dataPersistence.save(basicData, filename="basic_data")

    # 4.2. worldwide COVID statistics
    dataLoader.setQueryParams(
        fields=["date", "location_key", "country_name", "population", "population_female", "population_male",
                "new_confirmed", "new_deceased", "cumulative_confirmed", "cumulative_deceased", "cumulative_tested",
                "new_persons_vaccinated", "cumulative_persons_vaccinated", "new_persons_fully_vaccinated",
                "cumulative_persons_fully_vaccinated", "new_vaccine_doses_administered",
                "cumulative_vaccine_doses_administered", "new_recovered", "cumulative_recovered",
                "new_hospitalized_patients", "cumulative_hospitalized_patients", "new_intensive_care_patients",
                "cumulative_intensive_care_patients", "cumulative_confirmed_male", "cumulative_confirmed_female",
                "cumulative_deceased_male", "cumulative_deceased_female", "new_tested_male", "new_tested_female",
                "cumulative_tested_male", "cumulative_tested_female", "cumulative_hospitalized_patients_male",
                "cumulative_hospitalized_patients_female"])

    covidStat: pd.DataFrame = dataLoader.getData()
    toFill = [
        "new_confirmed", "new_deceased", "cumulative_confirmed", "cumulative_deceased", "cumulative_tested",
        "new_persons_vaccinated", "cumulative_persons_vaccinated", "new_persons_fully_vaccinated",
        "cumulative_persons_fully_vaccinated", "new_vaccine_doses_administered",
        "cumulative_vaccine_doses_administered", "new_recovered", "cumulative_recovered",
        "new_hospitalized_patients", "cumulative_hospitalized_patients", "new_intensive_care_patients",
        "cumulative_intensive_care_patients", "cumulative_confirmed_male", "cumulative_confirmed_female",
        "cumulative_deceased_male", "cumulative_deceased_female", "new_tested_male", "new_tested_female",
        "cumulative_tested_male", "cumulative_tested_female", "cumulative_hospitalized_patients_male",
        "cumulative_hospitalized_patients_female"
    ]

    # remove duplicates and NaN values from covid statistics
    covidStat.drop_duplicates(inplace=True)
    for c in toFill:
        covidStat[c].fillna(0, inplace=True)
    covidData = covidStat.groupby("country_name")["new_confirmed"].sum().reset_index()
    removeNegative(covidStat)
    dataPersistence.save(covidStat, filename="covid_stats")

    # 4.3. highlight COVID mortality rates
    dataLoader.setQueryParams(
        fields=["date", "location_key", "country_name", "new_confirmed", "new_deceased", "cumulative_confirmed",
                "cumulative_deceased", "new_deceased_male", "new_deceased_female", "cumulative_deceased_male",
                "cumulative_deceased_female", "new_deceased_age_0", "new_deceased_age_1", "new_deceased_age_2",
                "new_deceased_age_3", "new_deceased_age_4", "new_deceased_age_5", "new_deceased_age_6",
                "new_deceased_age_7", "new_deceased_age_8", "new_deceased_age_9", "cumulative_deceased_age_0",
                "cumulative_deceased_age_1", "cumulative_deceased_age_2", "cumulative_deceased_age_3",
                "cumulative_deceased_age_4", "cumulative_deceased_age_5", "cumulative_deceased_age_6",
                "cumulative_deceased_age_7", "cumulative_deceased_age_8", "cumulative_deceased_age_9"])
    mortalityData: pd.DataFrame = dataLoader.getData()

    # remove duplicates and NaN values from mortality data
    mortalityData.drop_duplicates(inplace=True)

    toFill: List[str] = [
        "new_confirmed", "new_deceased", "cumulative_confirmed",
        "cumulative_deceased", "new_deceased_male", "new_deceased_female", "cumulative_deceased_male",
        "cumulative_deceased_female", "new_deceased_age_0", "new_deceased_age_1", "new_deceased_age_2",
        "new_deceased_age_3", "new_deceased_age_4", "new_deceased_age_5", "new_deceased_age_6",
        "new_deceased_age_7", "new_deceased_age_8", "new_deceased_age_9", "cumulative_deceased_age_0",
        "cumulative_deceased_age_1", "cumulative_deceased_age_2", "cumulative_deceased_age_3",
        "cumulative_deceased_age_4", "cumulative_deceased_age_5", "cumulative_deceased_age_6",
        "cumulative_deceased_age_7", "cumulative_deceased_age_8", "cumulative_deceased_age_9",
        "new_deceased_male", "new_deceased_female", "cumulative_deceased_male", "cumulative_deceased_female"
    ]
    for c in toFill:
        mortalityData[c] = mortalityData[c].fillna(0)
    mortalityData.dropna(inplace=True)

    mortalityData["mortality_rate"] = mortalityData["cumulative_deceased"] / mortalityData["cumulative_confirmed"]
    removeNegative(mortalityData)
    dataPersistence.save(mortalityData, filename="mortality_data")

    # 4.4. COVID trends and dependencies regarding vaccinations
    dataLoader.setQueryParams(
        fields=["location_key", "country_name", "date", "new_persons_vaccinated", "cumulative_persons_vaccinated",
                "new_persons_fully_vaccinated", "cumulative_persons_fully_vaccinated", "cumulative_ventilator_patients",
                "new_persons_fully_vaccinated_pfizer", "cumulative_persons_fully_vaccinated_pfizer",
                "new_vaccine_doses_administered_pfizer", "cumulative_vaccine_doses_administered_pfizer",
                "new_persons_fully_vaccinated_moderna", "cumulative_persons_fully_vaccinated_moderna",
                "new_vaccine_doses_administered_moderna", "cumulative_vaccine_doses_administered_moderna",
                "new_persons_fully_vaccinated_janssen", "cumulative_persons_fully_vaccinated_janssen",
                "new_vaccine_doses_administered_janssen", "cumulative_vaccine_doses_administered_janssen"])
    vaccinationData: pd.DataFrame = dataLoader.getData()
    nanCols = [
        "new_persons_fully_vaccinated_pfizer", "cumulative_persons_fully_vaccinated_pfizer",
        "new_vaccine_doses_administered_pfizer", "cumulative_vaccine_doses_administered_pfizer",
        "new_persons_fully_vaccinated_moderna", "cumulative_persons_fully_vaccinated_moderna",
        "new_vaccine_doses_administered_moderna", "cumulative_vaccine_doses_administered_moderna",
        "new_persons_fully_vaccinated_janssen", "cumulative_persons_fully_vaccinated_janssen",
        "new_vaccine_doses_administered_janssen", "cumulative_vaccine_doses_administered_janssen"
    ]

    # remove duplicates and NaN values from vaccination data
    vaccinationData.drop_duplicates(inplace=True)
    for c in nanCols:
        vaccinationData[c].fillna(0, inplace=True)
    removeNegative(vaccinationData)
    vaccinationData = vaccinationData.groupby("date").sum().reset_index()
    dataPersistence.save(vaccinationData, filename="vaccination_stats")

    # 4.5
    # nurses and physicians impact per country on COVID
    stat1.drop_duplicates(inplace=True)
    stat1.dropna(inplace=True)
    dataPersistence.save(stat1, filename="hospital_staff_stats")

    # effects of  testing_policy on cases of COVID
    stat5.drop_duplicates(inplace=True)
    stat5.dropna(inplace=True)
    dataPersistence.save(stat5, filename="testing_policy_stats")

    # stats of COVID per country split between genders
    stat3.drop_duplicates(inplace=True)
    stat3.dropna(inplace=True)
    dataPersistence.save(stat3, filename="gender_stats")

    # correlation of temperature with COVID cases
    stat6.drop_duplicates(inplace=True)
    stat6.dropna(inplace=True)
    dataPersistence.save(stat6, filename="temperature_stats")
