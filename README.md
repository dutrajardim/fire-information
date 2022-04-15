### Table of content

- What you will find here (The Project Scope)
- Explore and Assess the Data (The Project Scope)
- The ETL process and the data model
- Project structure

# What you will find here

In this repository I wrote some Airflow DAGs (Directed Acyclic Graph) scripts to gather data from manifold datasets of related subjects, process and store the data creating a data lake that integrates those data to support analysis on them. \
Considering that the main organizations distributes their resources by administrative areas, the major goal of this project is to make possible compare historical events as wild fire and flood by administrative areas.


## Datasets

- Global Historical Climatology Network daily (GHCNd)
- Fire Information for Resource Management System (FIRMS)
- Administrative boundaries from Open Street Map databases (OSM-Boundaries)

# Exploring and Assessing the Data

### Global Historical Climatology Network daily (GHCNd)

This dataset contains records from loads of land surface stations, and was integrated and processed by the [National Center for Environment Information (NCEI)](https://www.ncei.noaa.gov/). Some of the variables from the NCEI's dataset explored in this repository project are average temperature and volume of precipitation. \
The paper [An Overview of the Global Historical Climatology Network-Daily Database](https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/papers/menne-etal2012.pdf), by Matthew J. Menne, Imke Durre, Russell S. Vose, Byron E. Gleason, and Tamara G. Houston, offer a more detailed description about this dataset. As described in the referenced paper, it's not guarantee to have daily updates for all stations, and some of then have only historical data as we can observe in the chart below that shows in the first picture the stations available at Minas Gerais and in the second picture the stations that mede climate data available in April - 2022.  

![Availability of GHCN data](docs/images/charts/BRA_MG/availability_of_ghcn_data/2022_Belo%20Horizonte.svg "Availability of GHCN data")

The query used to plot this chart is available in the [Stations Analysis Notebook](https://github.com/dutrajardim/fire-information/blob/main/notebooks/stations_analysis.ipynb).

To see the repository of the GHCN dataset, you can follow [this link](https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/). This project extracts the data by file transfer protocol, and makes use of **by year** csv file that is describe in the [README](https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/readme-by_year.txt) file in the dataset repository.  

### Fire Information for Resource Management System (FIRMS)

This dataset makes available Near Real-Time (NRT) active fire and thermal anomalies data within 3 hours of satellite observation from the Moderate Resolution Imaging Spectroradiometer (MODIS) and the Visible Infrared Imaging Radiometer Suite (VIIRS) instruments. \
The [Firms FAG](https://earthdata.nasa.gov/faq/firms-faq#ed-user-guides) describes the available data and how to download it. In this project was created a dag that extract data older then two months (archive data) by requesting a link throughout the [FIRMS form](https://firms.modaps.eosdis.nasa.gov/download/create.php), this link need to be set as an Airflow Variable. Another dag was created to download daily text files for the last two months via HTTPS and depends on a token to access the data. This both dags extract old data and is expected to be executed once. \
The major dag of this project, that is executed daily, also makes use of the token as a requirement of the FIRMS dataset.
The chart below shows a example of the data extracted from this datasets and the query is available in the [Firms Analysis Notebook](https://github.com/dutrajardim/fire-information/blob/main/notebooks/firms_analysis.ipynb).

![Plotting fire spots by month](docs/images/charts/BRA_MG/plotting_fire_spots/year_2021/month_12.svg "Plotting fire spots by month")

### Administrative boundaries from Open Street Map databases (OSM-Boundaries)

OSM Boundaries is a project aimed to make easily to extract administrative boundaries such as country boarders or equivalents from the OpenStreetMaps databases. An user account of [OpenStreetMap](https://www.openstreetmap.org/user/new) is required to download the data, and so is a token to execute the DAGs of this project. Geo shapes extracted from this dataset are used here to connect the data through administrative areas, so then we can create queries using this shapes.
Different sources make up the dataset, so we can expect that some events (for example, fire spots) can be duplicated on join process, as the location of the event can be within those overlapping shapes boundaries.

## The ETL process and the data model