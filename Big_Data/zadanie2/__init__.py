from utils import removeNegative, sameCols
from CSVDataDAO import CSVDataDAO
import pandas as pd
from collections import defaultdict


def zadanie2() -> None:
    print("Zadanie 2")
    dataPersistence = CSVDataDAO("result", zadanie=2)

    ######################### część 1 #########################

    dtypes = defaultdict(float, dict({
        "country_name": "string", "population": int,
        "population_female": int, "population_male": int,
        "area_qa_km": "float64", "population_density": "float64",
        "cumulative_confirmed": int, "cumulative_deceased": int
    }))
    # dtypes = defaultdict(float, dict({"country_name": "string"}))
    basic_data: pd.DataFrame = dataPersistence.load("basic_data", zadanie=1,
                                                    dtypes=dtypes).dropna()
    covid_stats: pd.DataFrame = dataPersistence.load("covid_stats", zadanie=1, dtypes=dtypes).dropna()
    mortality_data: pd.DataFrame = dataPersistence.load("mortality_data", zadanie=1, dtypes=dtypes).dropna()
    vaccination_stats: pd.DataFrame = dataPersistence.load("vaccination_stats", zadanie=1, dtypes=dtypes).dropna()

    for c in [basic_data, covid_stats, mortality_data, vaccination_stats]:
        for i in ["population_male", "population_female", "population", "cumulative_confirmed", "cumulative_deceased"]:
            try:
                c[i] = c[i].astype('int')
            except ValueError:
                try:
                    c[i] = c[i].apply(lambda x: x.replace("utf-8", "0")).astype('int')
                except AttributeError:
                    pass
            except KeyError:
                pass
            except AttributeError:
                pass

    cumulative_plain = basic_data.merge(covid_stats, on=[
        "date", "country_name", "population", "population_female", "location_key",
        "population_male", "cumulative_confirmed", "cumulative_deceased"
    ], suffixes=("b", "c"))

    cumulative_plain = cumulative_plain.merge(mortality_data, on=sameCols(cumulative_plain, mortality_data),
                                              suffixes=("c", "m"))

    removeNegative(cumulative_plain)
    cumulative_plain.drop_duplicates(inplace=True, ignore_index=True)
    cumulative_plain.fillna(0, inplace=True)
    dataPersistence.save(cumulative_plain, filename="cumulative_plain")

    cumulative_date = vaccination_stats.merge(mortality_data.groupby("date").sum(), on="date", suffixes=("v", "m"))
    removeNegative(cumulative_date)
    cumulative_date.drop_duplicates(inplace=True, ignore_index=True)
    cumulative_date.fillna(0, inplace=True)
    dataPersistence.save(cumulative_date, filename="cumulative_date")

    ######################### część 2 #########################

    # 2.1 world_countries
    world_countries: pd.DataFrame = dataPersistence.load("world_countries")
    combined_world_countries = world_countries.set_index("Country/Territory").merge(cumulative_plain.set_index("country_name"),
                                                                                    left_index=True, right_index=True, how="inner")
    dataPersistence.save(combined_world_countries, filename="combined_world_countries")

    # 2.2 gdp
    gdp: pd.DataFrame = dataPersistence.load("gdp")
    gdp.rename(columns={"Value": "gdp_value"}, inplace=True)
    combined_gdp = gdp.set_index("Country Name").merge(cumulative_plain.set_index("country_name"),
                                                       left_index=True, right_index=True, how="inner")

    dataPersistence.save(combined_gdp, filename="combined_gdp")

    # z części dodatkowej
    dataPersistence.load("hospital_staff_stats", zadanie=1, dtypes=dtypes)
    dataPersistence.load("testing_policy_stats", zadanie=1, dtypes=dtypes)
    dataPersistence.load("gender_stats", zadanie=1, dtypes=dtypes)
    dataPersistence.load("temperature_stats", zadanie=1, dtypes=dtypes)



