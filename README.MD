What evidence exists for temporal variability in Arctic terrestrial and freshwater biodiversity throughout the Holocene? A systematic map.
===

[![deploy thalloo maps](https://github.com/AndrewIOM/holocene-arctic-biodiversity-map/actions/workflows/thalloo.yml/badge.svg)](https://github.com/AndrewIOM/holocene-arctic-biodiversity-map/actions/workflows/thalloo.yml)

**Note. This repository contains the systematic map database, into which information is currently being coded. You may view the current progress in the visualisations linked to below.**

### [View the latest maps here](http://acm.im/holocene-arctic-biodiversity-map/)

**Background.** The Arctic tundra is subject to the greatest climate change-induced temperature rises of any biome. Both terrestrial and freshwater biota are responding to recent climate warming through variability in their distribution, abundance, and richness. However, uncertainty arises within models of future change when considering processes that operate over centennial timescales. A systematic evidence synthesis of centennial-scale variability in biodiversity does not currently exist for the Arctic biome. Here, we sought to address the primary research question: what evidence exists for temporal variability in Arctic terrestrial and freshwater biodiversity throughout the Holocene (11,650 years before present (yBP) – 0yBP)?

**Methods.** Consultation with stakeholders informed key definitions, scoping and the appropriateness of the research question. The research question was structured using a PECO framework – Arctic biota (P), a timestamped year in the Holocene (E), another year in the Holocene (C), and the dimensions of biodiversity that have been measured (O) – to inform the search strategy. Search strings were benchmarked against a test list of 100 known sources to ensure a specific and comprehensive return of literature. Searches will occur across 13 bibliographic databases. The eligibility criteria specify that sources must: (a) use ‘proxy’ methods to measure biodiversity; (b) fall within the spatial extent of the contemporary Arctic tundra biome; and (c) consist of a time-series that overlaps with 11,650yBP to 0yBP (1950AD). Information coded from studies will include proxy-specific information to account for both temporal uncertainty (i.e., the characteristics of age-depth models and dating methods) and taxonomic uncertainty (i.e., the samples and processes used for taxonomic identification). We will assess temporal uncertainty within each source by determining the quality of dating methods and measures; this information will be used to harmonise dates onto the IntCal20 calibration curve and determine the available temporal resolution and extent of evidence through space. Key outputs of this systematic map will be: (1) a graph database containing the spatial-temporal properties of each study dataset with taxonomic harmonisation; and (2) a geographical map of the evidence base. 

Imported metadata from other databases
---

A portion of the metadata included in this database was imported that previously coded within existing databases, specifically Neotoma and the International Tree Ring Database.

Dataset metadata were obtained from the Neotoma Paleoecology Database (http://www.neotomadb.org) and its constituent database(s): European Pollen Database; North American Pollen Database; and Pollen Database of Siberia and the Russian Far East. The work of data contributors, data stewards, and the Neotoma and in particular the EPD/ALPADABA community is gratefully acknowledged.

License
---

This Holocene Arctic Biodiversity Database is made available under the Open Database License: http://opendatacommons.org/licenses/odbl/1.0/. Any rights in individual contents of the database are licensed under the Database Contents License: http://opendatacommons.org/licenses/dbcl/1.0/

Instructions for Data Coders
---

Welcome to the working respository for full-text screening and data coding for our systematic map.

We are using a custom-designed app to conduct data coding. In this repository, you will find the following:

* **A file-based graph database in ```/data```.** The file-based structure allows for data coders to commit their changes every so often and resolve any conflicts between the coders.

* **The ready-to-run application in ```/dist```.** The app is compiled for macOS (Intel binary) and Windows.

### Tutorial

To perform coding, the following steps are required:

1. Sign up for a GitHub account (if you don't have one)

2. Get added as a collaborator to the repository AndrewIOM/holocene-arctic-biodiversity-map

3. Download the GitHub Desktop app and clone the repository to a local folder.

4. To test out the functionality, switch to the 'test' branch in the github app.

5. To run the data coding software, look in the 'dist' folder of the repository in the macos and windows folders to find the appropriate app. When running the app, you need to paste in the directory path for where the 'data' folder is. On macos you can right click a folder in finder while holding the option key to get the option to 'copy path'. Similar happens on windows with ctrl or alt (can't remember which).

6. To code for real, switch to the 'main' branch in the github app and get coding. Commit and push changes every so often.

Data specification
---

The database schema is being aligned with the Dublin Core and its extensions (*DCMI metadata terms*).

### Defining a Timeline

| Property | Metadata URI | Values | Description |
| --- | --- | --- | --- |
| Type | ... | Continuous; Discontinuous | A continuous timeline has no hiatuses (breaks) within the temporal sequence over the temporal resolution. For example, a growth ring series is continuous with annual timesteps. |
| Resolution | ... | Regular; Irregular | A time-series is *Regular* when time-steps are even (i.e. the same width in time). For example, sedimentary layers are generally irregular, but may be regular if varves are present. Tree growth rings are also regular. 
| Timestep | ... | float(year) | If a regular resolution, a value representing the fractional years for each time-step. Most often 1.0 for annual. |
| Feature | ... | Wood anatomical feature; Varves; Bone or tooth growth; other | If *other*, a short text label for the morphological feature (excluding taxon names if present). |
| Temporal Extent (yongest and oldest dates, and associated uncertainty brackets) | ... | An 'old date' (see below table) | Dates in one of the available dating expressions: calendar dates as AD or BC; radiocarbon dates that are calibrated (in calibrated years before present - cal yr BP) or uncalibrated (BP). Dates should be entered as expressed by the authors of the source. If there are calibrated radiocarbon dates, these should be used over uncalibrated radiocarbon dates. *Note: dates will convert automatically to the best representation on our common timeline when you click 'Link'.*

### Old Dates

| Property | Metadata URI | Values | Description |
| --- | --- | --- | --- |
| B - bpDate | ... | integer | An uncalibrated radiocarbon date expressed as 'Before Present'. |
| CalYrBP - calibratedDate | ... | integer | An calibrated radiocarbon date that has been calibrated. |
| CalYrBP - calibrationTechnique | ... | short text (optional) | If specified, enter the name of the calibration curve used to calibrate the date e.g. IntCalXX |
| HistoryYearAD - calendarYear | http://purl.org/dc/terms/date | integer | A calendar year in AD |
| HistoryYearBC - calendarYear | http://purl.org/dc/terms/date | integer | A calendar year in BC |
