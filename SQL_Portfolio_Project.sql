/*
Covid 19 Data Exploration 

Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types

*/

create Database Portfolio_Project;
use Portfolio_Project;


CREATE TABLE CovidDeaths (
    iso_code VARCHAR(10) NULL,
    continent VARCHAR(50) NULL,
    location VARCHAR(100) NULL,

    date_data VARCHAR(20) NULL, 

    population BIGINT NULL,

    total_cases BIGINT NULL,
    new_cases BIGINT NULL,
    new_cases_smoothed DOUBLE NULL,

    total_deaths BIGINT NULL,
    new_deaths BIGINT NULL,
    new_deaths_smoothed DOUBLE NULL,

    total_cases_per_million DOUBLE NULL,
    new_cases_per_million DOUBLE NULL,
    new_cases_smoothed_per_million DOUBLE NULL,

    total_deaths_per_million DOUBLE NULL,
    new_deaths_per_million DOUBLE NULL,
    new_deaths_smoothed_per_million DOUBLE NULL,

    reproduction_rate DOUBLE NULL,

    icu_patients BIGINT NULL,
    icu_patients_per_million DOUBLE NULL,

    hosp_patients BIGINT NULL,
    hosp_patients_per_million DOUBLE NULL,

    weekly_icu_admissions DOUBLE NULL,
    weekly_icu_admissions_per_million DOUBLE NULL,

    weekly_hosp_admissions DOUBLE NULL,
    weekly_hosp_admissions_per_million DOUBLE NULL
);




CREATE TABLE covid_vaccination (
    iso_code VARCHAR(10) NULL,
    continent VARCHAR(50) NULL,
    location VARCHAR(100) NULL,

    date_data VARCHAR(20) NULL, 

    new_tests DOUBLE NULL,
    total_tests DOUBLE NULL,
    total_tests_per_thousand DOUBLE NULL,
    new_tests_per_thousand DOUBLE NULL,
    new_tests_smoothed DOUBLE NULL,
    new_tests_smoothed_per_thousand DOUBLE NULL,

    positive_rate DOUBLE NULL,
    tests_per_case DOUBLE NULL,
    tests_units VARCHAR(100) NULL,

    total_vaccinations DOUBLE NULL,
    people_vaccinated DOUBLE NULL,
    people_fully_vaccinated DOUBLE NULL,

    new_vaccinations DOUBLE NULL,
    new_vaccinations_smoothed DOUBLE NULL,

    total_vaccinations_per_hundred DOUBLE NULL,
    people_vaccinated_per_hundred DOUBLE NULL,
    people_fully_vaccinated_per_hundred DOUBLE NULL,
    new_vaccinations_smoothed_per_million DOUBLE NULL,

    stringency_index DOUBLE NULL,

    population_density DOUBLE NULL,
    median_age DOUBLE NULL,
    aged_65_older DOUBLE NULL,
    aged_70_older DOUBLE NULL,

    gdp_per_capita DOUBLE NULL,
    extreme_poverty DOUBLE NULL,

    cardiovasc_death_rate DOUBLE NULL,
    diabetes_prevalence DOUBLE NULL,

    female_smokers DOUBLE NULL,
    male_smokers DOUBLE NULL,

    handwashing_facilities DOUBLE NULL,
    hospital_beds_per_thousand DOUBLE NULL,

    life_expectancy DOUBLE NULL,
    human_development_index DOUBLE NULL
);

select * from covid_vaccination
limit 50;

-- convert date column to date data type in Covid_vaccination
alter table covid_vaccination
add column date_data_new date;

SET SQL_SAFE_UPDATEs =0;

update covid_vaccination
set date_data_new = str_to_date(date_data,"%d-%m-%Y");


select * from coviddeaths
limit 50;

-- convert date column to date data type in Coviddeaths
alter table coviddeaths
add column date_data_new date;

update coviddeaths
set date_data_new = str_to_date(date_data,"%d-%m-%Y");


select *
from covid_vaccination
order by 3,4;

select *
from coviddeaths
order by 3,4;


-- select data that needed for analysis

select Location, Date_data_new, Total_cases, New_cases, Total_deaths, Population
from coviddeaths
where continent is not null
order by 1,2;

-- Looking at Total_cases vs Total Deaths

select Location, Date_data_new, Total_cases, Total_deaths, round((Total_Deaths/Total_cases)*100,2) as DeathPercentage
from coviddeaths
where continent is not null
order by 1,2;

-- Looking at Total_cases vs Population
-- show what % of Population is affected by Covid

select Location, Date_data_new, Population, Total_cases, round((Total_cases/Population)*100,3) as CasePercentageVsPopulation
from coviddeaths
where continent is not null
order by 1,2;

-- Looking at countries highest infection rate compared to population

select Location, max(Population) as Population, max(Total_cases) as Total_Case , round((max(Total_cases)/max(Population))*100,3) as TotalCase_vs_Population
from coviddeaths
where continent is not null
group by Location
order by TotalCase_vs_Population desc;

-- Looking at highest mortality rate compared to population

select Location, max(Population) as Population, max(Total_deaths) as Mortality_count , round((max(Total_deaths)/max(Population))*100,3) as MortalityRate_vs_Population
from coviddeaths
where continent is not null
group by Location
order by MortalityRate_vs_Population desc;

-- Lets Look by continent wise

select Location, Population, max(Total_deaths) as Mortality_count , round((max(new_deaths)/Population)*100,3) as MortalityRate_vs_Population
from coviddeaths
where continent is null and Location not in("World","International","European Union")
group by Location, Population
order by MortalityRate_vs_Population desc;

-- Global Numbers

select Date_data_new, sum(New_cases) as Total_cases, sum(New_deaths) as Total_Deaths, round((sum(New_Deaths)/sum(New_cases))*100,2) as DeathPercentage
from coviddeaths
where continent is not null
group by Date_data_new
order by Date_data_new;

-- Overall Global Data

select sum(New_cases) as Total_cases, sum(New_deaths) as Total_Deaths, round((sum(New_Deaths)/sum(New_cases))*100,2) as DeathPercentage
from coviddeaths
where continent is not null;

-- connecting Covid Deaths & Covid Vaccinations
-- looking at total population vs total vaccination

select cd.continent, cd.location , cd.Date_data_new , cd.population as Total_population, cv.new_vaccinations as Total_vaccination,
	sum(cv.new_vaccinations) over(partition by cd.location order by cd.location,cd.Date_data_new asc) as Running_Total_Vaccination
from coviddeaths as cd inner join covid_vaccination as cv
on cd.location = cv.location and
cd.Date_data_new = cv. Date_data_new
where cd.continent is not null;
-- where cd.continent is not null and cd.location = "Canada";


-- find the total population vs total vaccination

-- using CTE's

With TotalPopu_vs_Vaccination_CTE as(
select cd.continent, cd.location , cd.Date_data_new , cd.population as Total_population, cv.new_vaccinations as Total_vaccination,
	sum(cv.new_vaccinations) over(partition by cd.location order by cd.location,cd.Date_data_new asc) as Running_Total_Vaccination
from coviddeaths as cd inner join covid_vaccination as cv
on cd.location = cv.location and
cd.Date_data_new = cv. Date_data_new
where cd.continent is not null)

select continent, location, Date_data_new as Date, Total_population, Total_vaccination, Running_Total_vaccination,
round((Running_Total_vaccination/Total_population)*100,2) as Vaccination_vs_population
from temp_CTE;


-- Temp Table

create temporary Table Temp_TotalPopu_vs_Vaccination as
select cd.continent, cd.location , cd.Date_data_new , cd.population as Total_population, cv.new_vaccinations as Total_vaccination,
	sum(cv.new_vaccinations) over(partition by cd.location order by cd.location,cd.Date_data_new asc) as Running_Total_Vaccination
from coviddeaths as cd inner join covid_vaccination as cv
on cd.location = cv.location and
cd.Date_data_new = cv. Date_data_new
where cd.continent is not null;


-- need to drop temp table

drop table Temp_TotalPopu_vs_Vaccination;

-- create temp table again without the where clause
create temporary Table Temp_TotalPopu_vs_Vaccination as
select cd.continent, cd.location , cd.Date_data_new , cd.population as Total_population, cv.new_vaccinations as Total_vaccination,
	sum(cv.new_vaccinations) over(partition by cd.location order by cd.location,cd.Date_data_new asc) as Running_Total_Vaccination
from coviddeaths as cd inner join covid_vaccination as cv
on cd.location = cv.location and
cd.Date_data_new = cv. Date_data_new;


select *, round((Running_Total_vaccination/Total_population)*100,2) as Vaccination_vs_population
from Temp_TotalPopu_vs_Vaccination
where continent is not null;

-- creating view to store data later for visualization

create view Temp_TotalPopu_vs_Vaccination as
select cd.continent, cd.location , cd.Date_data_new , cd.population as Total_population, cv.new_vaccinations as Total_vaccination,
	sum(cv.new_vaccinations) over(partition by cd.location order by cd.location,cd.Date_data_new asc) as Running_Total_Vaccination
from coviddeaths as cd inner join covid_vaccination as cv
on cd.location = cv.location and
cd.Date_data_new = cv. Date_data_new
where cd.continent is not null;


select * from Temp_TotalPopu_vs_Vaccination;