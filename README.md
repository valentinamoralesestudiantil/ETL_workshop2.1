# ETL_workshop2

**Valentina Morales Valencia (2240427)**

---

## 1. Data Profiling (EDA)

### General information of the dataset spotify_dataset

- Number of rows: 114000  
- Number of columns: 21  
- Total memory used (MiB): 50.6 MiB  

### Data Issues Summary

| Column_name | Data type | Missing values | % of missing values | Cardinality | Basic Statistics | duplicated data 
 | Inconsistencies in writing | Notes |Description |
|------|------|------------|
| Unnamed: 0 | int64 | 0 | 0.00 | ... | Count = 114000, Mean = 56999.5, Std = 32909, Min = 0, Max = 113999 | ... | ... | It is not considered relevant for the analysis, to eliminate |
| track_id | Duplicates | 24259 duplicates (should be unique) |
| artists | Inconsistency | Same artist written differently |
| album_name | Inconsistency | Same album written differently |
| track_name | Inconsistency | Same track written differently |
| loudness | Invalid values | Positive values when should be ≤ -1 |
| time_signature | Invalid values | Contains 0 (invalid) |

---

### Grammy Dataset

- Rows: 4810  
- Columns: 10  
- Memory: 3.1 MiB  

### Data Issues Summary

| Column | Issue | Description |
|------|------|------------|
| winner | Irrelevant | All values true |
| year | Wrong type | Should be object |
| category | Inconsistency | Same category written differently |
| artist | Missing | 38% missing |
| workers | Missing | 45% missing |
| img | Duplicates | Same image linked to multiple artists |

---

## 2. Data Cleaning (Spotify)

| Issue | Strategy | Justification |
|------|--------|--------------|
| Unnamed: 0 | Drop column | Not relevant |
| Null artists | Remove rows | Cannot infer |
| Null album_name | Remove rows | Cannot infer |
| Null track_name | Remove rows | Cannot infer |
| Loudness > -1 | Remove rows | Invalid values |
| time_signature = 0 | Remove rows | Invalid |
| Duplicate track_id | Reassign IDs | Preserve data |

### Summary

| Metric | Before | After |
|------|-------|------|
| Rows | 114000 | 112408 |
| Nulls | 3 | 0 |

---

## 3. Data Cleaning (Grammy)

| Issue | Strategy | Justification |
|------|--------|--------------|
| winner | Drop | Not useful |
| Null nominee | Remove | Cannot infer |
| Null artist | Remove | Cannot infer |
| Null workers | Remove | Cannot infer |
| Null img | Remove | Cannot infer |
| Duplicate img | Remove | Ensure consistency |

### Summary

| Metric | Before | After |
|------|-------|------|
| Rows | 4810 | 3983 |

---

## 4. Transformations

- Convert `mode` → boolean  
- Convert `year` → object  
- Normalize text columns (lowercase, trim, remove accents)  
- Create auxiliary columns for merging  
- Merge datasets (Spotify + Grammy)  

---

## 5. Star Schema

### Fact Table: `has_grammy_match`

- popularity  

---

### Dimension: `dim_music`

| Column |
|------|
| track_id |
| track_name |
| album_name |
| artists |
| energy |
| duration_ms |
| track_genre |
| danceability |
| valence |
| explicit |
| instrumentalness |
| acousticness |
| speechiness |
| loudness |
| mode |
| time_signature |
| liveness |
| tempo |
| music_key |

---

### Dimension: `dim_grammy`

| Column |
|------|
| grammy_category |
| grammy_workers |
| grammy_artist |
| grammy_img |
| grammy_title |
| grammy_nominee |

---

### Dimension: `dim_time`

| Column |
|------|
| grammy_year |
| grammy_published_at |
| grammy_updated_at |

---

## 6. Star Schema Diagram

![Star Schema](diagrama_de_estrella.png)

---

## 7. KPIs

| KPI | Description |
|----|------------|
| Total Spotify songs | Dataset size |
| Grammy winning songs | Songs with awards |
| Most popular Grammy songs | High popularity |
| Explicit content % | Content classification |
| Grammy songs by genre | Genre distribution |
| Grammy songs per year | Trend over time |
