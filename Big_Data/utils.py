import pandas as pd


def removeNegative(df):
    for key in [
        "population", "population_female", "population_male", "area_sq_km", "population_density",
        "cumulative_confirmed", "cumulative_deceased", "new_confirmed", "new_deceased", "cumulative_tested",
        "new_persons_vaccinated", "cumulative_persons_vaccinated", "new_persons_fully_vaccinated",
        "cumulative_persons_fully_vaccinated", "new_vaccine_doses_administered",
        "cumulative_vaccine_doses_administered", "new_recovered", "cumulative_recovered",
        "new_hospitalized_patients", "cumulative_hospitalized_patients", "new_intensive_care_patients",
        "cumulative_intensive_care_patients", "cumulative_confirmed_male", "cumulative_confirmed_female",
        "cumulative_deceased_male", "cumulative_deceased_female", "new_tested_male", "new_tested_female",
        "cumulative_tested_male", "cumulative_tested_female", "cumulative_hospitalized_patients_male",
        "cumulative_hospitalized_patients_female", "new_deceased_male", "new_deceased_female",
        "new_deceased_age_0", "new_deceased_age_1", "new_deceased_age_2", "new_deceased_age_3",
        "new_deceased_age_4", "new_deceased_age_5", "new_deceased_age_6", "new_deceased_age_7",
        "new_deceased_age_8", "new_deceased_age_9", "cumulative_deceased_age_0", "cumulative_deceased_age_1",
        "cumulative_deceased_age_2", "cumulative_deceased_age_3", "cumulative_deceased_age_4",
        "cumulative_deceased_age_5", "cumulative_deceased_age_6", "cumulative_deceased_age_7",
        "cumulative_deceased_age_8", "cumulative_deceased_age_9", "new_deceased_male_1",
        "new_deceased_female_1", "cumulative_deceased_male_1", "cumulative_deceased_female_1"
    ]:
        try:
            df.loc[df[key] < 0, key] = 0
        except AttributeError:
            pass
        except KeyError:
            pass


def sameCols(x: pd.DataFrame, y: pd.DataFrame) -> list[str]:
    res = []
    for c in x.columns:
        if c in y and not c.__contains__("Unnamed"):
            res.append(c)
    return res
