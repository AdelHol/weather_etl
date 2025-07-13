# Delta Green

## Zadání

### Část 1: Sběr dat o počasí

Vytvořte **ETL pipeline** s využitím nástroje pro orchestraci jobů (např. **Dagster**, **Prefect** nebo jiný dle vašeho výběru).

**Cíl:**  
Data budou sloužit datovému analytikovi pro následující účely:
- Vytvoření **dashboardu**, kde bude porovnávat predikce s realitou.
- Jako **vstup do dalších výpočtů**, které budou využívat poslední verzi predikce v libovolném čase.

**Zdroj:**  
Využijte **API z webu [WeatherAPI.com](https://www.weatherapi.com/)**.  
Pro přístup k API se zaregistrujte (postačí e-mailová adresa).  
Po registraci začne běžet **14denní zkušební verze**, která se po uplynutí automaticky přepne na **bezplatný plán**.

**Frekvence:**  
Každých **15 minut** stáhněte aktuální data o počasí a jeho **predikci na následujících 48 hodin**.

**Lokality:**  
- Praha  
- Londýn

**Ukládání dat:**  
Data vhodně uložte do **databáze**. Ideální volbou je **PostgreSQL spuštěný v Dockeru**.  
Nemusíte se zabývat databázovými migracemi, postačí, když budou tabulky vytvořeny jednorázově pomocí `.sql` souboru.

**Dostupnost dat:**  
Data by měla být k dispozici pro uvedené účely s **minimálními časovými prodlevami**.

---

### Část 2: Načtení dat

Vytvořte **Jupyter Notebook**, ve kterém načtete data z databáze s **poslední verzí predikce pro každou hodinu**.

---

## Řešení

Projekt je rozdělen do několika částí:

### 1. ETL pipeline (Dagster)

- Kód najdete ve složce [`weather_etl/`](./weather_etl)
- Spouští se pravidelně a:
  - Stahuje data z WeatherAPI (aktuální i forecast)
  - Ukládá je do PostgreSQL databáze (včetně zpětného doplnění 15min hodnot)
  -  [`weather_etl/README.md`](./weather_etl/README.md)

### 2. Databáze

- Používá se **PostgreSQL** (např. běžící přes Docker)
- Tabulky se vytváří pomocí skriptu [`sql/create_schema_and_tables.sql`](./weather_etl/sql/create_schema_and_tables.sql)

###  3. Analýza dat

- Notebook [`notebooks/comparison.ipynb`](./notebooks/comparison.ipynb) provádí:
  - Načtení aktuálních a predikovaných dat
  - Výpočet chyb (MAE, RMSE, směrodatná odchylka)
  - Porovnání predikce s realitou
  - Vizualizace pomocí `plotly` 



