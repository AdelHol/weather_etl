# Databázové tabulky projektu Weather ETL

Tento dokument popisuje strukturu databázového schématu, které je využíváno ETL pipeline pro sběr a ukládání dat o počasí.

---

## Schéma: `reporting_data`

---

### Tabulka: `weather_current`

| Sloupec          | Typ               | Popis                              |
|------------------|-------------------|------------------------------------|
| id               | SERIAL PRIMARY KEY| Primární klíč                      |
| city             | VARCHAR(50)       | Název města                        |
| as_of            | TIMESTAMP         | Čas, ke kterému platí měření       |
| temp_c           | DOUBLE PRECISION  | Teplota v °C                       |
| temp_f           | DOUBLE PRECISION  | Teplota ve °F                      |
| is_day           | SMALLINT          | Den (1) / Noc (0)                  |
| condition_text   | TEXT              | Textový popis počasí               |
| condition_icon   | TEXT              | Ikonka počasí                      |
| condition_code   | INTEGER           | Kód podmínky počasí                |
| wind_mph         | DOUBLE PRECISION  | Rychlost větru (mph)               |
| wind_kph         | DOUBLE PRECISION  | Rychlost větru (kph)               |
| wind_degree      | INTEGER           | Směr větru ve stupních             |
| wind_dir         | VARCHAR(10)       | Zkratka směru větru (N, S, ...)    |
| pressure_mb      | DOUBLE PRECISION  | Tlak v mb                          |
| pressure_in      | DOUBLE PRECISION  | Tlak v in                          |
| precip_mm        | DOUBLE PRECISION  | Srážky v mm                        |
| precip_in        | DOUBLE PRECISION  | Srážky v in                        |
| humidity         | INTEGER           | Vlhkost (%)                        |
| cloud            | INTEGER           | Oblačnost (%)                      |
| feelslike_c      | DOUBLE PRECISION  | Pociťovaná teplota v °C            |
| feelslike_f      | DOUBLE PRECISION  | Pociťovaná teplota ve °F           |
| windchill_c      | DOUBLE PRECISION  | Chill faktor v °C                  |
| windchill_f      | DOUBLE PRECISION  | Chill faktor ve °F                 |
| heatindex_c      | DOUBLE PRECISION  | Heat index v °C                    |
| heatindex_f      | DOUBLE PRECISION  | Heat index ve °F                   |
| dewpoint_c       | DOUBLE PRECISION  | Rosný bod v °C                     |
| dewpoint_f       | DOUBLE PRECISION  | Rosný bod ve °F                    |
| vis_km           | DOUBLE PRECISION  | Viditelnost v km                   |
| vis_miles        | DOUBLE PRECISION  | Viditelnost v mílích               |
| gust_mph         | DOUBLE PRECISION  | Nárazy větru mph                   |
| gust_kph         | DOUBLE PRECISION  | Nárazy větru kph                   |
| uv               | DOUBLE PRECISION  | UV index                           |
| fetched_at       | TIMESTAMP         | Čas, kdy byl záznam získán         |

#### Indexy
- Unikátní index: **(city, as_of)**

---

### Tabulka: `weather_forecast`

| Sloupec                 | Typ               | Popis                               |
|-------------------------|-------------------|-------------------------------------|
| id                      | SERIAL PRIMARY KEY| Primární klíč                       |
| city                    | VARCHAR(50)       | Název města                         |
| forecast_for            | TIMESTAMP         | Čas předpovědi                      |
| temp_c                  | DOUBLE PRECISION  | Teplota v °C                        |
| temp_f                  | DOUBLE PRECISION  | Teplota ve °F                       |
| is_day                  | SMALLINT          | Den/Noc                             |
| condition_text          | TEXT              | Textový popis počasí                |
| condition_icon          | TEXT              | Ikonka počasí                       |
| condition_code          | INTEGER           | Kód podmínky počasí                 |
| wind_mph                | DOUBLE PRECISION  | Rychlost větru (mph)                |
| wind_kph                | DOUBLE PRECISION  | Rychlost větru (kph)                |
| wind_degree             | INTEGER           | Směr větru ve stupních              |
| wind_dir                | VARCHAR(10)       | Zkratka směru větru                 |
| pressure_mb             | DOUBLE PRECISION  | Tlak v mb                           |
| pressure_in             | DOUBLE PRECISION  | Tlak v in                           |
| precip_mm               | DOUBLE PRECISION  | Srážky v mm                         |
| precip_in               | DOUBLE PRECISION  | Srážky v in                         |
| humidity                | INTEGER           | Vlhkost (%)                         |
| cloud                   | INTEGER           | Oblačnost (%)                       |
| feelslike_c             | DOUBLE PRECISION  | Pociťovaná teplota v °C             |
| feelslike_f             | DOUBLE PRECISION  | Pociťovaná teplota ve °F            |
| windchill_c             | DOUBLE PRECISION  | Chill faktor v °C                   |
| windchill_f             | DOUBLE PRECISION  | Chill faktor ve °F                  |
| heatindex_c             | DOUBLE PRECISION  | Heat index v °C                     |
| heatindex_f             | DOUBLE PRECISION  | Heat index ve °F                    |
| dewpoint_c              | DOUBLE PRECISION  | Rosný bod v °C                      |
| dewpoint_f              | DOUBLE PRECISION  | Rosný bod ve °F                     |
| will_it_rain            | SMALLINT          | Bude pršet (0/1)                    |
| chance_of_rain          | SMALLINT          | Šance na déšť (%)                   |
| vis_km                  | DOUBLE PRECISION  | Viditelnost v km                    |
| vis_miles               | DOUBLE PRECISION  | Viditelnost v mílích                |
| gust_mph                | DOUBLE PRECISION  | Nárazy větru mph                    |
| gust_kph                | DOUBLE PRECISION  | Nárazy větru kph                    |
| uv                      | DOUBLE PRECISION  | UV index                            |
| prediction_generated_at | TIMESTAMP         | Kdy byla predikce vygenerována      |
| fetched_at              | TIMESTAMP         | Čas, kdy byl záznam získán          |

#### Indexy
- Unikátní index: **(city, forecast_for, prediction_generated_at)**



