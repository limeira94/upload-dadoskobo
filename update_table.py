#!/usr/bin/env python
import argparse
import os
import tempfile
import shutil
from urllib.parse import urlparse
import geopandas as gpd
import pandas as pd
import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv


def load_geospatial_file(filepath):
    _, ext = os.path.splitext(filepath.lower())

    if ext == '.zip':
        temp_dir = tempfile.mkdtemp()
        try:
            shutil.unpack_archive(filepath, temp_dir)
            shp_path = None
            for f in os.listdir(temp_dir):
                if f.endswith(".shp"):
                    shp_path = os.path.join(temp_dir, f)
                    break
            if not shp_path:
                raise ValueError("No .shp found in ZIP.")
            gdf = gpd.read_file(shp_path)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    elif ext == '.geojson':
        gdf = gpd.read_file(filepath)

    elif ext == '.gpkg':
        gdf = gpd.read_file(filepath, layer=0)

    else:
        raise ValueError("Unsupported file format (use .zip, .geojson, .gpkg).")

    if gdf.crs is None:
        raise ValueError("File has no CRS defined.")
    gdf = gdf.to_crs(epsg=4326)
    return gdf


def sanitize_gdf_columns(gdf):
    def sanitize_column_name(name):
        return (name.strip()
                   .lower()
                   .replace(" ", "_")
                   .replace("-", "_")
                   .replace("/", "_")
                   .replace(".", "_"))
    gdf.columns = [sanitize_column_name(c) for c in gdf.columns]
    return gdf


def check_schema_compatibility(conn, table_name, gdf):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
        """, (table_name,))
        columns_info = cur.fetchall()

    db_columns = {row[0].lower() for row in columns_info}
    gdf_geom_col = gdf.geometry.name.lower() if gdf.geometry is not None else None
    gdf_cols = [
        c.lower() for c in gdf.columns 
        if c.lower() not in ["fid", "id", gdf_geom_col]
    ]
    for col in gdf_cols:
        if col not in db_columns:
            raise Exception(f"Column '{col}' does not exist in table '{table_name}'.")
    return True


def truncate_table(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
    conn.commit()


def insert_geodata(conn, table_name, gdf):
    has_id_in_gdf = any(col.lower() == 'id' for col in gdf.columns)
    geom_col_name = 'geom'

    columns_to_insert = []
    if has_id_in_gdf:
        columns_to_insert.append('id')

    geometry_col = gdf.geometry.name.lower()
    for col in gdf.columns:
        col_lower = col.lower()
        if col_lower in [geometry_col, 'fid']:
            continue
        if col_lower == 'id' and has_id_in_gdf:
            continue
        columns_to_insert.append(col)

    placeholders = ", ".join(["%s" for _ in columns_to_insert])
    insert_sql = f"""
        INSERT INTO {table_name} ({", ".join(columns_to_insert)}, {geom_col_name})
        VALUES ({placeholders}, ST_GeomFromText(%s, 4326))
    """

    generate_id = not has_id_in_gdf
    next_id = 1
    BATCH_SIZE = 1000
    num_rows = len(gdf)

    for start_idx in range(0, num_rows, BATCH_SIZE):
        end_idx = min(start_idx + BATCH_SIZE, num_rows)
        subset = gdf.iloc[start_idx:end_idx]
        with conn.cursor() as cur:
            for _, row in subset.iterrows():
                geom = row.geometry
                wkt = geom.wkt if geom and not geom.is_empty else None

                values = []
                for c in columns_to_insert:
                    if c.lower() == 'id' and has_id_in_gdf:
                        val = row[c]
                    elif c.lower() == 'id' and generate_id:
                        val = next_id
                        next_id += 1
                    else:
                        val = row[c]
                    # Convert NaNs to None
                    if pd.isna(val):
                        val = None
                    values.append(val)

                params = values + [wkt]
                cur.execute(insert_sql, params)
        conn.commit()
        print(f"Inserted rows {start_idx} to {end_idx} into {table_name}.")


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Update a PostGIS table with new data from a geospatial file (using DATABASE_URL from .env)."
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Target table name (e.g. kobodata_iniciativakobo)"
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to the geospatial file"
    )
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL not found in environment. Make sure .env has DATABASE_URL=...")

    parsed = urlparse(database_url)
    user = parsed.username
    password = parsed.password
    host = parsed.hostname
    port = parsed.port
    dbname = parsed.path.lstrip("/")

    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    try:
        print(f"Loading file: {args.file}")
        gdf = load_geospatial_file(args.file)

        gdf = sanitize_gdf_columns(gdf)

        check_schema_compatibility(conn, args.table, gdf)
        print("Schema compatibility OK.")

        print(f"Truncating table '{args.table}'...")
        truncate_table(conn, args.table)

        print(f"Inserting data into '{args.table}'...")
        insert_geodata(conn, args.table, gdf)

        print(f"\nTable '{args.table}' updated successfully.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
