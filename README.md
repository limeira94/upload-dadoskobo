# Upload Kobo Data (PostGIS)

This project allows you to **upload and update geospatial data** into a **PostGIS database** using **Python**.  
The script reads files in **Shapefile (.zip), GeoJSON (.geojson), and GeoPackage (.gpkg)** formats, sanitizes column names,  
checks schema compatibility, truncates the target table, and inserts new data.

---

## Features

- **Reads geospatial data** from Shapefile (`.zip`), GeoJSON (`.geojson`), and GeoPackage (`.gpkg`).
- **Sanitizes column names** for consistency.
- **Checks schema compatibility** with the database table.
- **Truncates the existing table** before inserting new data.
- **Inserts geospatial data** into a PostgreSQL/PostGIS database.

---

## Setup Instructions

```bash
git clone https://github.com/limeira94/upload-dadoskobo.git
cd upload-dadoskobo

python -m venv venv

venv\Scripts\activate

source venv/bin/activate

pip install -r requirements.txt
```

create a .env file with the following content:
`DATABASE_URL=postgres://postgres:1234@localhost:5432/geoportal_new`

execute the following command to create the table in the database:
`python update_table.py --table kobodata_iniciativakobo --file data/my_geopackage.gpkg`
