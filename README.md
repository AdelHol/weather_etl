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

