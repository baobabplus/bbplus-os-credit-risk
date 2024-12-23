## Introduction
This data pipeline is shared by Baobab+, and intended for actors of the pay as you go industry.
It enables to build various datasets, visualizations and projections on customer repayment data.
This containts the logic to build 
- A core dataset *accounts_history*  containing individual account's daily data & KPIs (beginner and advanced versions).
- Cohort Visualizations
- Cohort Repayment Projection Methodology
- Baobab+ IFRS9 ECL Projection Methodology

Data shared in this repository is artificial and anonimyzed. 

## Get it running
This data pipeline has been built using the framework dbt, with the cloud datawarehouse BigQuery. 

To install & run dbt - please use the following commands (full guide here : https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup)

If you have python & pip installed:

`pip install dbt-core dbt-bigquery`

Once installed, run:

`dbt init`

To configure your auth method. 
Finally run 

`dbt compile`

`dbt seed`

`dbt run`


If you are not using dbt & BigQuery: the code is mostly SQL, and should adapt to various SQL data warehouses.


## Disclaimer
This code is provided "as is," without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and noninfringement. In no event shall the authors be liable for any claim, damages, or other liability, whether in an action of contract, tort, or otherwise, arising from, out of, or in connection with the code or the use or other dealings in the code.
Use at your own risk.

