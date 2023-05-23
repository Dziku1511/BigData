from typing import List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from CSVDataDAO import CSVDataDAO


def makeBoxPlots(graphs, rows=1, cols=3):
    for i, graph in enumerate(graphs):
        ax = plt.subplot(rows, cols, i + 1)
        try:
            v = np.log10(graph['values'] + 1.1)
            ax.set_yscale(graph['yscale'])
        except KeyError:
            v = graph['values']
        ax.set_title(graph['title'])
        ax.boxplot(v)
    plt.show()


def removeOutliers(df: pd.Series) -> pd.Series:
    Q1 = df.quantile(0.25)
    Q3 = df.quantile(0.75)
    IQR = Q3 - Q1
    return df.loc[~((df < (Q1 - 1.5 * IQR)) | (df > (Q3 + 1.5 * IQR)))]


def subMake(stats: List[Tuple[str, str, pd.DataFrame]]):
    graphs = []
    for (t, k, s) in stats:
        graphs.append({"title": f"Skala logarytmiczna: {t}", "values": s[k], "yscale": "log"})
        graphs.append({"title": f"Dane surowe: {t}", "values": s[k]})
        graphs.append({"title": f"Dane wyczyszczone: {t}", "values": removeOutliers(s[k])})
    return graphs


def zadanie3() -> None:
    print("Zadanie 3")
    dataPersistence = CSVDataDAO("result", zadanie=3)

    # ==================== część 1 ====================

    cumulative_plain: pd.DataFrame = dataPersistence.load("cumulative_plain", zadanie=2)
    covid_stats: pd.DataFrame = dataPersistence.load("covid_stats", zadanie=1)
    cumulative_date: pd.DataFrame = dataPersistence.load("cumulative_date", zadanie=2)
    combined_gdp: pd.DataFrame = dataPersistence.load("combined_gdp", zadanie=2)
    combined_world_countries: pd.DataFrame = dataPersistence.load("combined_world_countries", zadanie=2)
    basic_data: pd.DataFrame = dataPersistence.load("basic_data", zadanie=1)
    hospital_staff_stats: pd.DataFrame = dataPersistence.load("hospital_staff_stats", zadanie=1)
    temperature_stats: pd.DataFrame = dataPersistence.load("temperature_stats", zadanie=1)

    fig = plt.figure()

    # 1.1
    stats: List[Tuple[str, str, pd.DataFrame]] = [
        ("nowe przypadki", "new_confirmed", cumulative_plain),
        ("nowe śmierci", "new_deceased", cumulative_plain),
        ("osoby nowo zaszczepione ", "new_persons_vaccinated", cumulative_plain),
        ("populacja", "population", cumulative_plain),
        ("wartość GDP", "gdp_value", combined_gdp),
        # dodatkowe
        ("powierzchnia", "area_sq_km", combined_gdp),
        ("osoby wyzdrowiałe", "new_recovered", combined_gdp),
        ("śmiertelność", "mortality_rate", combined_world_countries),
        ("osoby z założoną wentylacją", "cumulative_ventilator_patients", cumulative_date),
        ("suma osób w pełni zaszczepionych", "cumulative_persons_fully_vaccinated", covid_stats),
    ]
    graphs = subMake(stats)
    makeBoxPlots(graphs[0:15], 5, 3)
    makeBoxPlots(graphs[15:], 5, 3)

    # 1.2

    # ================ część 2 ================
    date_column = 'date'
    weekly = pd.Grouper(key=date_column, freq='W-MON')
    semi_monthly = pd.Grouper(key=date_column, freq='SM')
    monthly = pd.Grouper(key=date_column, freq='MS')
    quarterly = pd.Grouper(key=date_column, freq='QS')
    working_data: pd.DataFrame = cumulative_plain[cumulative_plain["country_name"] == "Germany"]
    working_data.loc[:, date_column] = pd.to_datetime(working_data[date_column])

    # 2.1.1
    data_per_week: pd.Series = working_data.groupby(weekly)["new_confirmed"].sum()
    data_per_month: pd.Series = working_data.groupby(monthly)["new_confirmed"].sum()
    data_per_quarter: pd.Series = working_data.groupby(quarterly)["new_confirmed"].sum()
    tmp = [
        ("confirmed per week", data_per_week),
        ("confirmed per month", data_per_month),
        ("confirmed per quarter", data_per_quarter)
    ]
    for i, (title, data) in enumerate(tmp):
        data.plot(title=title, rot=45)
        plt.show()

    # 2.1.2
    location_key = "Country Code"
    sum_confirmed: pd.Series = combined_gdp.groupby([location_key])["new_confirmed"] \
        .sum().rename("confirmed")
    sum_vaccinated: pd.Series = combined_gdp.groupby([location_key])["new_persons_vaccinated"] \
        .sum().rename("vaccinated")
    sum_deceased: pd.Series = combined_gdp.groupby([location_key])["new_deceased"] \
        .sum().rename("deceased")
    population: pd.Series = combined_gdp.groupby([location_key])["population"].mean()
    gdp: pd.Series = combined_gdp.groupby([location_key])["gdp_value"].mean()

    t = [sum_confirmed, sum_vaccinated, sum_deceased, population, gdp]
    stats: pd.DataFrame = pd.concat(t, axis=1)
    stats["conf_population"] = stats["confirmed"] / stats["population"]
    stats["vacc_population"] = stats["vaccinated"] / stats["population"]
    stats["dead_population"] = stats["deceased"] / stats["population"]
    stats["conf_gdp"] = stats["confirmed"] / stats["gdp_value"]
    stats["vacc_gpd"] = stats["vaccinated"] / stats["gdp_value"]
    stats["dead_gpd"] = stats["deceased"] / stats["gdp_value"]
    dataPersistence.save(stats, filename="country_population_stats")

    # 2.1.3
    vaccination_month: pd.Series = working_data.groupby(monthly)["new_persons_vaccinated"].sum()
    vaccination_semimonth: pd.Series = working_data.groupby(semi_monthly)["new_persons_vaccinated"].sum()
    vaccination_semimonth.plot(title="Szczepienia w czasie (pół miesiąca)", rot=45)
    plt.show()
    vaccination_month.plot(title="Szczepienia w czasie (miesiąc)", rot=45)
    plt.show()

    # 2.1.4
    weekly_vaccinated: pd.Series = working_data.groupby(weekly)["new_persons_vaccinated"].sum()
    weekly_confirmed: pd.Series = working_data.groupby(weekly)["new_confirmed"].sum()
    weekly_dead: pd.Series = working_data.groupby(weekly)["new_deceased"].sum()
    vacc_effect: pd.DataFrame = pd.concat([weekly_dead, weekly_confirmed, weekly_vaccinated], axis=1)
    vacc_effect['effect_dead'] = weekly_vaccinated.cov(weekly_dead)
    vacc_effect['effect_confirmed'] = weekly_vaccinated.cov(weekly_confirmed)
    dataPersistence.save(vacc_effect, filename="vacc_effectiveness")

    # 2.1.5
    med_confirmed: pd.Series = working_data.groupby(monthly)["new_confirmed"].median()
    med_dead: pd.Series = working_data.groupby(monthly)["new_deceased"].median()
    med_vacc: pd.Series = working_data.groupby(monthly)["new_persons_vaccinated"].median()
    population: pd.Series = (working_data.groupby(monthly)["population"].mean() / 1000000).rename("population_mil")
    med_per_population: pd.Dataframe = pd.concat([med_confirmed, med_dead, med_vacc, population], axis=1)
    med_per_population['confirmed_per_mil'] = med_confirmed / population
    med_per_population['dead_per_mil'] = med_dead / population
    med_per_population['vacc_per_mil'] = med_vacc / population
    dataPersistence.save(med_per_population, filename="med_per_population")
    med_per_population.plot(y=['confirmed_per_mil', 'dead_per_mil', 'vacc_per_mil'], title="Mediana", rot=45)
    plt.show()

    # 2.1.6
    std = working_data[["new_confirmed", "new_deceased", "new_persons_vaccinated"]].std()
    dataPersistence.save(std, filename="newX_std")

    # 2.2
    working_data1: pd.DataFrame = combined_world_countries[combined_world_countries["CCA3"] == "DEU"]
    working_data1.loc[:, date_column] = pd.to_datetime(working_data1[date_column])
    weekly_dead: pd.Series = working_data1.groupby(weekly)["new_deceased"].sum()
    weekly_confirmed: pd.Series = working_data1.groupby(weekly)["new_confirmed"].sum()

    # stat1
    weekly_hospitalized: pd.Series = working_data.groupby(weekly)["new_hospitalized_patients"].sum()
    weekly_intensive: pd.Series = working_data.groupby(weekly)["new_intensive_care_patients"].sum()
    df: pd.DataFrame = pd.concat([weekly_hospitalized, weekly_intensive, weekly_vaccinated], axis=1)
    df['cov_hosp_conf'] = weekly_vaccinated.cov(weekly_hospitalized)
    df['cov_int_conf'] = weekly_vaccinated.cov(weekly_intensive)
    df['cov_int_hosp'] = weekly_hospitalized.cov(weekly_intensive)
    df.plot(title="cov{vacc, (hospitalized, intensive care)} i cov{hospitalized, intensive care}",
            y=["cov_int_conf", "cov_hosp_conf", "cov_int_hosp"])
    plt.show()

    # stat2
    area: pd.Series = working_data1.groupby(weekly)["area_sq_km"].mean()
    density: pd.Series = working_data1.groupby(weekly)["population_density"].mean()
    df = pd.concat([area, density], axis=1)
    df["cov_dead_area"] = area.cov(weekly_dead)
    df["cov_dead_density"] = density.cov(weekly_dead)
    df.plot(title="cov{dead, (area, density)}", y=["cov_dead_area", "cov_dead_density"])
    plt.show()

    # stat3
    data_per_week: pd.Series = working_data.groupby(weekly)["new_recovered"].sum()
    data_per_month: pd.Series = working_data.groupby(monthly)["new_recovered"].sum()
    data_per_quarter: pd.Series = working_data.groupby(quarterly)["new_recovered"].sum()
    for i, (title, data) in enumerate([
        ("recovered per week", data_per_week),
        ("recovered per month", data_per_month),
        ("recovered per quarter", data_per_quarter)
    ]):
        data.plot(title=title, rot=45)
        plt.show()

    # stat4
    df = pd.concat([area, density, weekly_confirmed], axis=1)
    df["confirmed_per_area"] = weekly_confirmed / area
    df["confirmed_per_density"] = weekly_confirmed / density
    df.plot(title="confirmed per (area, density)", y=["confirmed_per_area", "confirmed_per_density"])
    plt.show()

    # stat5
    dead_female: pd.Series = working_data1.groupby(weekly)["new_deceased_female"].sum()
    dead_male: pd.Series = working_data1.groupby(weekly)["new_deceased_male"].sum()
    df: pd.DataFrame = pd.concat([dead_male, dead_female], axis=1)
    df.plot(title="diff genders")
    plt.show()

    # stat6
    sum_vacc: pd.Series = combined_gdp.groupby([location_key])["new_persons_fully_vaccinated"] \
        .sum().rename("persons_fully_vaccinated")
    sum_admin: pd.Series = combined_gdp.groupby([location_key])["new_vaccine_doses_administered"] \
        .sum().rename("vaccine_doses_administered")
    gdp: pd.Series = combined_gdp.groupby([location_key])["gdp_value"].mean()

    t = [sum_admin, sum_vacc, gdp]
    stats: pd.DataFrame = pd.concat(t, axis=1)
    stats["vacc_gdp"] = stats["persons_fully_vaccinated"] / stats["gdp_value"]
    stats["administered_gpd"] = stats["vaccine_doses_administered"] / stats["gdp_value"]
    dataPersistence.save(stats, filename="country_gdp_vacc")

    # stat7
    plt.bar(hospital_staff_stats["country_name"],
            hospital_staff_stats["avg_nurses_per_1000"] + hospital_staff_stats["avg_physicians_per_1000"])
    plt.show()

    # stat8
    temperature_stats.plot(x="temp", rot=45, y=["sum_new_confirmed", "sum_new_deceased", "sum_population"])
    plt.show()

    # stat9
    wealth_confirmed: pd.Series = combined_gdp.groupby("gdp_value")["new_confirmed"].mean()
    wealth_confirmed.plot(title="GDP vs new cases")
    plt.show()

    # stat10
    ts: pd.Series = working_data.groupby(weekly)["new_confirmed"].sum()
    plt.hist(ts.values, bins=10)
    plt.show()


    # ==================== część 3 ====================

    # 3.1.1 nowe zachorowania na 100 osób
    norm: pd.Series = covid_stats['new_confirmed'] / 100
    plt.bar(norm.index, norm.values)
    plt.title('new_confirmed')

    # 3.1.2 nowe szczepienia na 100 osób
    norm: pd.Series = covid_stats['new_persons_vaccinated'] / 100
    plt.bar(norm.index, norm.values)
    plt.title('new_persons_vaccinated')

    # 3.1.3 nowe śmierci na 100 osób
    norm: pd.Series = covid_stats['new_deceased'] / 100
    plt.bar(norm.index, norm.values)
    plt.title('new_deceased')

    # 3.2.1 PKB na 100 osób
    norm: pd.Series = combined_gdp['gdp_value'] / 100
    plt.bar(norm.index, norm.values)
    plt.title('gdp_value')

    # 3.2.2 nowe zachorowania na 100 osób
    norm: pd.Series = covid_stats['new_recovered'] / 100
    plt.bar(norm.index, norm.values)
    plt.title('new_recovered')

    # 3.2.3 śmiertelność na 100 osób
    norm: pd.Series = cumulative_plain['mortality_rate'] / 100
    plt.bar(norm.index, norm.values)
    plt.title('mortality_rate')

    # 3.2.4 nowi pacjenci wymagający szczególnej opieki na 100 osób
    norm: pd.Series = covid_stats['new_intensive_care_patients'] / 100
    plt.bar(norm.index, norm.values)
    plt.title('new_intensive_care_patients')

    # 3.2.5 łączne śmierci na 1000 osób
    norm: pd.Series = covid_stats['cumulative_confirmed'] / 1000
    plt.bar(norm.index, norm.values)
    plt.title('cumulative_confirmed')

    # ==================== część 4 ====================

    # 4.1.1 nowe zachorowania a nowe szczepienia
    covid_stats[['new_confirmed', 'new_persons_vaccinated']].corr()
    # nowe zachorowania a nowe zgony
    covid_stats[['new_confirmed', 'new_deceased']].corr()
    # nowe zgony a nowe szczepienia
    covid_stats[['new_deceased', 'new_persons_vaccinated']].corr()

    # 4.1.2 nowe zachorowania a łączne zachorowania
    covid_stats[['new_confirmed', 'cumulative_confirmed']].corr()
    # nowe zachorowania a łączne szczepienia
    covid_stats[['new_confirmed', 'cumulative_persons_vaccinated']].corr()
    # nowe zachorowania a łączne zgony
    covid_stats[['new_confirmed', 'cumulative_deceased']].corr()
    # nowe szczepienia a łączne zachorowania
    covid_stats[['new_persons_vaccinated', 'cumulative_confirmed']].corr()
    # nowe szczepienia a łączne szczepienia
    covid_stats[['new_persons_vaccinated', 'cumulative_persons_vaccinated']].corr()
    # nowe szczepienia a łączne zgony
    covid_stats[['new_persons_vaccinated', 'cumulative_deceased']].corr()
    # nowe zgony a łączne zachorowania
    covid_stats[['new_deceased', 'cumulative_confirmed']].corr()
    # nowe zgony a łączne szczepienia
    covid_stats[['new_deceased', 'cumulative_persons_vaccinated']].corr()
    # nowe zgony a łączne zgony
    covid_stats[['new_deceased', 'cumulative_deceased']].corr()

    # 4.1.3 PKB a nowe zachorowania, zgony, sczepienia
    merged = pd.merge(covid_stats, combined_gdp[['date', 'gdp_value']], on='date')
    correlation = merged['gdp_value'].corr(merged['new_confirmed', 'new_deceased', 'new_persons_vaccinated'])
    # 4.1.4 nowe zgony, szczepienia, zachorowania a populacja
    covid_stats[['new_confirmed', 'new_deceased', 'new_persons_vaccinated', 'population']].corr()

    # 4.2.1 nowe zachorowania a nowe śmierci, populacja
    covid_stats[['new_confirmed', 'new_deceased', 'population']].corr()
    # 4.2.2 nowe zachorowania, nowe szczepienia a łączne zachorowania i łączenie szczepienia
    covid_stats[
        ['new_confirmed', 'new_persons_vaccinated', 'cumulative_persons_vaccinated', 'cumulative_confirmed']].corr()
    # 4.2.3 łączne zachorowania a łączne śmierci
    covid_stats[['cumulative_confirmed', 'cumulative_deceased']].corr()
    # 4.2.4 nowe śmierci a łączne śmierci
    covid_stats[['new_deceased', 'cumulative_deceased']].corr()
    # 4.2.5 populacja ogólnie a populacja male/female
    basic_data[['population', 'population_female', 'population_male']].corr()
