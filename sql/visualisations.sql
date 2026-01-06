-- Graf 1: Top 10 najdlhších vodných tokov
SELECT 
    w.watercourse_name,
    SUM(f.length) AS total_length
FROM UK_WATERS.fact_watercourse_links f
JOIN UK_WATERS.dim_watercourse w
    ON f.dim_watercourse_id = w.watercourse_id
WHERE w.watercourse_name IS NOT NULL
GROUP BY w.watercourse_name
ORDER BY total_length DESC
LIMIT 10;

-- Graf 2: Počet úsekov podľa smeru toku
SELECT 
    f.flow_direction,
    COUNT(*) AS total_links
FROM UK_WATERS.fact_watercourse_links f
GROUP BY f.flow_direction
ORDER BY total_links DESC;

-- Graf 3: Priemerná dĺžka úsekov podľa typu toku (form)
SELECT 
    w.form,
    AVG(f.length) AS avg_length
FROM UK_WATERS.fact_watercourse_links f
JOIN UK_WATERS.dim_watercourse w
    ON f.dim_watercourse_id = w.watercourse_id
GROUP BY w.form
ORDER BY avg_length DESC;

-- Graf 4: Počet vodných tokov podľa kategórie hydro uzlov (start node)
SELECT 
    n.hydro_node_category,
    COUNT(*) AS total_links
FROM UK_WATERS.fact_watercourse_links f
JOIN UK_WATERS.dim_hydro_node n
    ON f.start_node_id = n.hydro_node_id
GROUP BY n.hydro_node_category
ORDER BY total_links DESC;

-- Graf 5: Počet vodných tokov podľa formy toku
SELECT 
    w.form,
    COUNT(*) AS total_links
FROM UK_WATERS.fact_watercourse_links f
JOIN UK_WATERS.dim_watercourse w
    ON f.dim_watercourse_id = w.watercourse_id
GROUP BY w.form
ORDER BY total_links DESC;

-- Graf 6: Top 10 vodných tokov podľa počtu spojení
SELECT 
    w.watercourse_name AS watercourse,
    COUNT(*) AS total_links
FROM UK_WATERS.fact_watercourse_links f
JOIN UK_WATERS.dim_watercourse w
    ON f.dim_watercourse_id = w.watercourse_id
GROUP BY w.watercourse_name
ORDER BY total_links DESC
LIMIT 10;
