CREATE DATABASE IF NOT EXISTS HIPPO_UK_WATERS;
CREATE SCHEMA IF NOT EXISTS UK_WATERS;
CREATE SCHEMA IF NOT EXISTS HIPPO_UK_WATERS;

-- Raw tables
CREATE OR REPLACE TABLE HYDRO_NODE_RAW AS
SELECT *
FROM HIPPO_UK_WATERS_DB.PRS_OPENRIVERS_SCH.PRS_HYDRO_NODE_VW;

CREATE OR REPLACE TABLE WATERCOURSE_LINK_RAW AS
SELECT *
FROM HIPPO_UK_WATERS_DB.PRS_OPENRIVERS_SCH.PRS_WATERCOURSE_LINK_VW;

-- Dimension: Hydro Node
CREATE OR REPLACE TABLE UK_WATERS.dim_hydro_node AS
SELECT DISTINCT
    id AS hydro_node_id,
    hydro_node_category,
    geometry,
    geography
FROM UK_WATERS.hydro_node_raw;

-- Dimension: Watercourse
CREATE OR REPLACE TABLE UK_WATERS.dim_watercourse AS
SELECT DISTINCT
    id AS watercourse_id,
    watercourse_name,
    watercourse_name_alternative,
    form,
    fictitious
FROM UK_WATERS.watercourse_link_raw;

-- Dimension: Flow Direction
CREATE OR REPLACE TABLE UK_WATERS.dim_flow_direction AS
SELECT DISTINCT
    flow_direction AS flow_dir_id,
    flow_direction
FROM UK_WATERS.watercourse_link_raw;

-- Dimension: Length Bucket
CREATE OR REPLACE TABLE UK_WATERS.dim_length_bucket AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY length) AS length_id,
    CASE 
        WHEN length < 500 THEN 'Short'
        WHEN length BETWEEN 500 AND 2000 THEN 'Medium'
        ELSE 'Long'
    END AS length_bucket
FROM UK_WATERS.watercourse_link_raw;

-- Fact Table: Watercourse Links
CREATE OR REPLACE TABLE UK_WATERS.fact_watercourse_links AS
SELECT 
    f.id AS fact_link_id,
    f.id AS link_id,
    f.start_node AS start_node_id,
    f.end_node AS end_node_id,
    f.id AS dim_watercourse_id,
    f.flow_direction AS dim_flow_dir_id,
    ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.length DESC) AS dim_length_id,
    f.length,
    RANK() OVER (PARTITION BY f.start_node ORDER BY f.length DESC) AS rank_by_length
FROM UK_WATERS.watercourse_link_raw f;
