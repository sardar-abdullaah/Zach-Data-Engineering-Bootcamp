-- Create a custom data type for films with various attributes
CREATE TYPE films AS (
    year INTEGER,
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

-- Create a custom enum type to represent quality classification
CREATE TYPE quality_class AS
     ENUM ('bad', 'average', 'good', 'star');
    
-- Create a table to store actors and their associated information
CREATE TABLE actors (
    actor TEXT,
    actor_id TEXT,
    films_info films[], -- Array of film details
    quality_class quality_class, -- Quality classification
    current_year INTEGER, -- Year of data
    is_active BOOLEAN -- Whether the actor is currently active
);

-- =====================================
-- Cumulative Table Generation Query
-- =====================================

-- Fetch data for actors from the previous year (2006)
WITH last_year AS (
    SELECT * 
    FROM actors
    WHERE current_year = 2006
),

-- Fetch actor film data for the current year (2007)
this_year AS (
    SELECT * 
    FROM actor_films
    WHERE year = 2007
),

-- Consolidate film data for the current year and calculate quality classification
consolidated_films AS (
    SELECT 
        af.actor, 
        af.actorid,
        ARRAY_AGG(ROW(af.year, af.film, af.votes, af.rating, af.filmid)::films) AS films_info, -- Aggregate films into an array
        CASE        
            WHEN AVG(af.rating) > 8 THEN 'star'
            WHEN AVG(af.rating) > 7 THEN 'good'
            WHEN AVG(af.rating) > 6 THEN 'average'
            ELSE 'bad'
        END::quality_class AS quality, -- Determine quality based on average rating
        MAX(af.year) AS current_year -- Get the most recent year
    FROM this_year af
    GROUP BY af.actor, af.actorid
)

-- Insert consolidated data into the actors table
INSERT INTO actors
SELECT 
    COALESCE(cf.actor, a.actor) AS actor, -- Use actor from consolidated films or previous year
    COALESCE(cf.actorid, a.actor_id) AS actor_id,
    COALESCE(a.films_info, ARRAY[]::films[]) || COALESCE(cf.films_info, ARRAY[]::films[]) AS films_info, -- Combine films from both years
    COALESCE(cf.quality, a.quality_class) AS quality, -- Use the latest quality classification
    COALESCE(cf.current_year, a.current_year + 1) AS current_year, -- Increment year for continuing actors
    cf.actor IS NOT NULL AS is_active -- Mark as active if present in consolidated data
FROM last_year a
FULL OUTER JOIN consolidated_films cf
    ON a.actor = cf.actor;

-- =====================================
-- Backfill Query for Actors History SCD
-- =====================================

-- Create a table to store historical records for actors
CREATE TABLE actors_history_scd (
    actor TEXT,
    quality_class QUALITY_CLASS,
    is_active BOOLEAN,
    start_date INTEGER, -- Start year for a record
    end_date INTEGER, -- End year for a record
    current_year INTEGER -- Current year of the record
);

-- Step 1: Identify streaks of unchanged records by comparing with lagged values
WITH actor AS (
    SELECT 
        actor,
        current_year,
        quality_class, 
        is_active,
        LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) AS previous_quality_class, -- Previous quality
        LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) AS previous_is_active -- Previous activity status
    FROM actors
    WHERE current_year <= 2006
),

-- Step 2: Determine where streaks begin based on changes in quality or activity
streak_indicator AS (
    SELECT 
        actor,
        current_year,
        quality_class, 
        is_active, 
        CASE 
            WHEN quality_class <> previous_quality_class THEN 1
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END AS streak_indicator
    FROM actor
),

-- Step 3: Accumulate streaks for continuous periods of unchanged records
streaks AS (
    SELECT 
        actor,
        current_year,
        quality_class, 
        is_active,
        SUM(streak_indicator) OVER (PARTITION BY actor ORDER BY current_year) AS streaks
    FROM streak_indicator
)

-- Insert summarized records into actors_history_scd
INSERT INTO actors_history_scd 
SELECT 
    actor,
    quality_class, 
    is_active,
    MIN(current_year) AS start_date, -- Start of the streak
    MAX(current_year) AS end_date, -- End of the streak
    2006 AS current_year -- Reference year
FROM streaks
GROUP BY actor, streaks, is_active, quality_class
ORDER BY actor, MIN(current_year);

-- =====================================
-- Incremental Query for Actors History SCD
-- =====================================

-- Create a type to represent SCD changes
CREATE TYPE actors_scd_type AS (
    quality_class QUALITY_CLASS,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER
);

-- Step 1: Fetch today's data and historical data
WITH today_date AS (
    SELECT * 
    FROM actors_history_scd 
    WHERE end_date = 2006
      AND current_year = 2006
),
historical_data AS (
    SELECT * 
    FROM actors_history_scd 
    WHERE end_date <> 2006
      AND current_year = 2006
),

-- Step 2: Fetch data for the current season (2007)
this_season_data AS (
    SELECT * 
    FROM actors
    WHERE current_year = 2007
),

-- Step 3: Identify unchanged records
unchanged_records AS (
    SELECT 
        td.actor,
        ts.quality_class,
        ts.is_active,
        td.start_date,
        ts.current_year AS end_date,
        ts.current_year
    FROM this_season_data ts
    LEFT JOIN today_date td 
        ON ts.actor = td.actor 
    WHERE ts.quality_class = td.quality_class 
      AND ts.is_active = td.is_active
),

-- Step 4: Identify new records
new_records AS (
    SELECT 
        ts.actor,
        ts.quality_class,
        ts.is_active,
        ts.current_year AS start_date,
        ts.current_year AS end_date,
        ts.current_year
    FROM this_season_data ts
    LEFT JOIN today_date td 
        ON ts.actor = td.actor 
    WHERE td.actor IS NULL
),

-- Step 5: Identify changed records and unnest them into separate rows
changed_records AS (
    SELECT 
        td.actor,
        UNNEST(ARRAY[
            ROW(td.quality_class, td.is_active, td.start_date, td.end_date)::actors_scd_type,
            ROW(ts.quality_class, ts.is_active, ts.current_year, ts.current_year)::actors_scd_type
        ]) AS records,
        ts.current_year
    FROM this_season_data ts
    LEFT JOIN today_date td 
        ON ts.actor = td.actor 
    WHERE ts.quality_class <> td.quality_class 
       OR ts.is_active <> td.is_active
),
unnested_change_records AS (
    SELECT 
        actor,
        (records::actors_scd_type).quality_class,
        (records::actors_scd_type).is_active,
        (records::actors_scd_type).start_date,
        (records::actors_scd_type).end_date,
        current_year
    FROM changed_records
)

-- Combine historical data, unchanged records, new records, and changed records
SELECT * 
FROM historical_data
UNION ALL
SELECT * 
FROM unnested_change_records
UNION ALL
SELECT * 
FROM unchanged_records
UNION ALL
SELECT * 
FROM new_records;
