# ETL_workshop2

**Valentina Morales Valencia (2240427)**

---

## 1. Data Profiling (EDA)

### General information of the dataset spotify_dataset

- Number of rows: 114000  
- Number of columns: 21  
- Total memory used (MiB): 50.6 MiB  

| column_name | Data type | Missing values | % of missing values | Cardinality | Basic Statistics | duplicated data | Inconsistencies in writing | Notes |
|------------|----------|---------------|----------------------|-------------|------------------|-----------------|----------------------------|------|
| Unnamed: 0 | int64 | 0 | 0.00 | ... | Count = 114000<br>Mean = 56999.5<br>Std = 32909<br>Min = 0<br>Max = 113999 | ... | ... | It is not considered relevant for the analysis, to eliminate |
| track_id | object | 0 | 0.00 | 89741 | ... | Unexpected result: There are 24259 duplicates in the column | ... | It is the identifier of the song, it must be unique, so when it is repeated it shows an inconsistency. |
| artists | object | 1 | 0.0009 | 31438 | ... | Duplicate expected | Data are found that represent the same artist but written differently | They are the artists of a song |
| album_name | object | 1 | 0.0009 | 46590 | ... | Duplicate expected | Data are found that represent the same album_name but written differently | It's the name of the album |
| track_name | object | 1 | 0.0009 | 73609 | ... | Duplicate expected | Data are found that represent the same track_name but written differently | It's the name of the song |
| popularity | int64 | 0 | 0.00 | ... | Count = 114000<br>Mean = 33<br>Std = 22<br>Min = 0<br>Max = 100 | ... | ... | You can see that the data are approximately centered, which means that most songs have low or medium popularity and only some reach a high level of popularity represented as outliers. |
| duration_ms | int64 | 0 | 0.00 | ... | Count = 114000<br>Mean = 2<br>Std = 1<br>Min = 0<br>Max = 5 | ... | ... | It represents the duration in milliseconds, you can see many higher atypical values due to longer durations than usual, but it does not represent an error. |
| explicit | bool | 0 | 0.00 | ... | ... | ... | ... | Indicates if the song contains explicit content |
| danceability | float64 | 0 | 0.00 | ... | Count = 114000<br>Mean = 0.56<br>Std = 0.17<br>Min = 0<br>Max = 0.98 | ... | ... | It indicates how danceable the song is, you can see that most of the songs have a medium-high danceability, so the low values are presented as outliers. |
| energy | float64 | 0 | 0.00 | ... | Count = 114000<br>Mean = 0.64<br>Std = 0.25<br>Min = 0<br>Max = 1 | ... | ... | Energy level or intensity, you can see the presence of a wide dispersion and that the dataset is composed largely of energetic songs |
| key | int64 | 0 | 0.00 | ... | Count = 114000<br>Mean = 5.3<br>Std = 3.5<br>Min = 0<br>Max = 11 | ... | ... | Musical tonality encoded in number, it is seen as presenting a low dispersion and no outliers that exceed the 12 existing tones are presented |
| loudness | float64 | 0 | 0.00 | ... | Count = 114000<br>Mean = -8<br>Std = 5<br>Min = -49.5<br>Max = 4.5 | ... | ... | Average volume, as indicated by Spotify should be maximum -1 but positive numbers and many very low outliers are present. |
| mode | int64 | 0 | 0.00 | ... | Count = 114000<br>Mean = 0.6<br>Std = 0.48<br>Min = 0<br>Max = 1 | ... | ... | Musical mode where 1 is greater and 0 is smaller although the boxplot is not the best representation for this because it is badly classified as int64 although it represents a bool |
| speechiness | float64 | 0 | 0.00 | ... | Count = 114000<br>Mean = 0.08<br>Std = 0.1<br>Min = 0<br>Max = 0.96 | ... | ... | It shows how much spoken content the track has in the graph you can see that the average is in a very low range which indicates that most have little spoken content but many high outliers are presented in it. |
| acousticness | float64 | 0 | 0.00 | ... | Count = 114000<br>Mean = 0.3<br>Std = 0.3<br>Min = 0<br>Max = 0.99 | ... | ... | It shows the probability that it is acoustic, in it there is a lot of dispersion of the data but little acoustic songs predominate but you can see a very long queue that highly acoustic songs represent |
| instrumentalness | float64 | 0 | 0.00 | ... | Count = 114000<br>Mean = 0.15<br>Std = 0.3<br>Min = 0<br>Max = 1 | ... | ... | It shows the probability that it is instrumental, in it you can see a very small box which shows that the data are not so scattered and has little instrumental songs but also contain many superior outliers that indicate that there are some songs with a lot of instrumental |
| liveness | float64 | 0 | 0.00 | ... | Count = 114000<br>Mean = 0.2<br>Std = 0.19<br>Min = 0<br>Max = 1 | ... | ... | It shows the probability that it looks like a live recording, in it you can see that the average is very low but there are very high outliers present close to 1 in this you can see that the absence of a live recording predominates |
| valence | float64 | 0 | 0.00 | ... | Count = 114000<br>Mean = 0.47<br>Std = 0.25<br>Min = 0<br>Max = 0.99 | ... | ... | It shows how positive or cheerful it sounds, in this case the box contains a high dispersion and is very centered so it can be concluded that most of the songs have an intermediate level of being positive. |
| tempo | float64 | 0 | 0.00 | ... | Count = 114000<br>Mean = 122<br>Std = 29.9<br>Min = 0<br>Max = 243 | ... | ... | It shows the speed of the song in BPM, in which no relevant data is seen out of the ordinary for the most part. |
| time_signature | int64 | 0 | 0.00 | ... | Count = 114000<br>Mean = 3.9<br>Std = 0.4<br>Min = 0<br>Max = 5 | ... | ... | It shows the musical compass, in the graph you can see how most focus on a compass of 4 so it does not show much dispersion and has few outliers among them those located in 0 that are an inconsistency within the music |
| track_genre | object | 0 | 0.00 | 114 | ... | Duplicate expected | ... | Musical genre |

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
