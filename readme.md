gcp-metadata
============

## Program to read Google Bigquery Medata

If you are looking out for a code that gives you metadata of Google BigQuery Tables, you are at right place.
This utility uses Python 2.7 and latest Google Client libraries as of May-2019.

## Installation Instructions:
* Clone this repository to your local
* Create a new virtual environment with Python 2.7 Interpreter
* Install requirements; use command : `pip install -r requirements.txt`
* Do not forget to set-up environment variable to use service account : `GOOGLE_APPLICATION_CREDENTIALS=your-service-account-key.json`

## Configurations:
* All configurations are kept in properties.conf. You need to change them as per your need
* `projects` : contains comma seperated list of Bigquery projects
* `datasets` : contains list of all datasets
* `tables_metadata_file_name` : name of the file where table metadata will be stored
* `attributes_metadata_file_name` : name of the file where attribute metadata will be stored 

Feel free to play with the code and add more features as you need. Happy coding!
