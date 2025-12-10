# Advanced NASS Query Examples

## Important Note

⚠️ **These advanced queries may not always return data**. The USDA NASS database has varying data availability depending on:
- Commodity type
- Geographic area (state/county)
- Year
- Statistic type
- Special classifications (organic, etc.)

**Always test queries first** on https://quickstats.nass.usda.gov/ before adding them to your scripts.

## Basic Reliable Queries (Tested & Working)

These queries consistently return data:

### Corn Production by State
```python
{
    "commodity_desc": "CORN",
    "year": "2022",
    "state_alpha": "IA",
    "statisticcat_desc": "PRODUCTION"
}
```

### Wheat Yield
```python
{
    "commodity_desc": "WHEAT",
    "year": "2022",
    "state_alpha": "KS",
    "statisticcat_desc": "YIELD"
}
```

### Soybean Acres Planted
```python
{
    "commodity_desc": "SOYBEANS",
    "year": "2022",
    "state_alpha": "IL",
    "statisticcat_desc": "AREA PLANTED"
}
```

### Cattle Inventory
```python
{
    "commodity_desc": "CATTLE",
    "year": "2022",
    "state_alpha": "TX",
    "statisticcat_desc": "INVENTORY"
}
```

## Advanced Queries (Test First!)

### Organic Production

⚠️ Organic data is limited and may not be available for all commodities/years.

**Working Example (when data exists):**
```python
{
    "commodity_desc": "SOYBEANS",
    "prodn_practice_desc": "ORGANIC",  # Use this instead of class_desc
    "year": "2019",  # Older years more likely to have data
    "state_alpha": "IA",
    "statisticcat_desc": "AREA HARVESTED"
}
```

**Alternative Organic Query:**
```python
{
    "commodity_desc": "CORN",
    "prodn_practice_desc": "ORGANIC",
    "year": "2016",
    "statisticcat_desc": "AREA PLANTED",
    "agg_level_desc": "NATIONAL"  # National level may be more available
}
```

### County-Level Queries

⚠️ County-level data availability varies significantly.

**Working Example (when data exists):**
```python
{
    "commodity_desc": "CORN",
    "state_alpha": "IA",
    "county_name": "STORY",  # Must be exact county name in CAPS
    "year": "2022",
    "statisticcat_desc": "AREA PLANTED",
    "agg_level_desc": "COUNTY"
}
```

**Tips for County Queries:**
- Not all commodities have county-level data
- Major crop states (IA, IL, IN, NE) more likely to have data
- Use `AREA PLANTED` or `AREA HARVESTED` rather than `PRODUCTION`
- County names must be all caps and exact

### Price Queries

⚠️ Price data has different structure and availability.

**Working Example (when data exists):**
```python
{
    "commodity_desc": "CORN",
    "state_alpha": "IA",
    "statisticcat_desc": "PRICE RECEIVED",
    "year": "2022",
    "freq_desc": "MONTHLY",  # Monthly data more available
    "reference_period_desc": "FIRST OF MONTH"
}
```

**Simpler Price Query:**
```python
{
    "commodity_desc": "CATTLE",
    "state_alpha": "TX",
    "statisticcat_desc": "PRICE RECEIVED",
    "year": "2022",
    "unit_desc": "$ / CWT"
}
```

### Irrigation Data

⚠️ Very limited availability.

**Working Example (when data exists):**
```python
{
    "commodity_desc": "CORN",
    "state_alpha": "NE",
    "prodn_practice_desc": "IRRIGATED",
    "statisticcat_desc": "AREA HARVESTED",
    "year": "2018"  # Census years (2012, 2017) more likely
}
```

### Fruit and Vegetable Production

⚠️ Use specific fruit/vegetable names, not "VEGETABLE TOTALS".

**Working Examples:**
```python
# Apples
{
    "commodity_desc": "APPLES",
    "state_alpha": "WA",
    "statisticcat_desc": "PRODUCTION",
    "year": "2022"
}

# Lettuce
{
    "commodity_desc": "LETTUCE",
    "state_alpha": "CA",
    "statisticcat_desc": "PRODUCTION",
    "year": "2022"
}

# Potatoes
{
    "commodity_desc": "POTATOES",
    "state_alpha": "ID",
    "statisticcat_desc": "PRODUCTION",
    "year": "2022"
}
```

### Multi-Commodity Comparisons

To query multiple commodities, run separate queries:

```python
commodities = ["CORN", "SOYBEANS", "WHEAT"]
for commodity in commodities:
    result = execute_query({
        "commodity_desc": commodity,
        "year": "2022",
        "state_alpha": "IA",
        "statisticcat_desc": "PRODUCTION"
    })
```

### Time Series Data

For multiple years, query each year separately:

```python
years = ["2020", "2021", "2022"]
for year in years:
    result = execute_query({
        "commodity_desc": "CORN",
        "year": year,
        "state_alpha": "IA",
        "statisticcat_desc": "PRODUCTION"
    })
```

## Common Commodities (Reliable Data)

### Major Crops
- `CORN`
- `SOYBEANS`
- `WHEAT`
- `COTTON`
- `RICE`
- `BARLEY`
- `OATS`
- `SORGHUM`

### Livestock
- `CATTLE`
- `HOGS`
- `SHEEP`
- `GOATS`
- `CHICKENS`

### Dairy
- `MILK`

### Fruits (State-Specific)
- `APPLES` (WA, NY, MI)
- `ORANGES` (FL, CA)
- `GRAPES` (CA)
- `STRAWBERRIES` (CA)

### Vegetables (State-Specific)
- `LETTUCE` (CA, AZ)
- `TOMATOES` (CA, FL)
- `POTATOES` (ID, WA)
- `ONIONS` (CA, WA)

## Common Statistics Categories

### Crop Statistics
- `PRODUCTION` - Total production
- `YIELD` - Production per acre
- `AREA PLANTED` - Acres planted
- `AREA HARVESTED` - Acres harvested
- `AREA BEARING` - For fruits/nuts

### Livestock Statistics
- `INVENTORY` - Number of animals
- `PRODUCTION` - Meat/milk production
- `SALES` - Number sold
- `PRICE RECEIVED` - Price per unit

## Debugging Failed Queries

### Step 1: Simplify
Remove all optional parameters and test with just the basics:
```python
{
    "commodity_desc": "CORN",
    "year": "2022"
}
```

### Step 2: Add State
```python
{
    "commodity_desc": "CORN",
    "year": "2022",
    "state_alpha": "IA"
}
```

### Step 3: Add Statistic
```python
{
    "commodity_desc": "CORN",
    "year": "2022",
    "state_alpha": "IA",
    "statisticcat_desc": "PRODUCTION"
}
```

### Step 4: Test on NASS Website
Before adding to your script:
1. Go to https://quickstats.nass.usda.gov/
2. Select parameters using dropdowns
3. Click "Get Data"
4. If data appears, the query will work in the script

## Why Queries Fail

### Common Reasons:
1. **Data doesn't exist** - Not all commodities tracked in all states
2. **Wrong year** - Some data has publication delays
3. **Invalid commodity name** - Must match NASS exactly (use NASS website to find correct names)
4. **Invalid statistic** - Not all statistics available for all commodities
5. **Missing required parameters** - Some queries need `agg_level_desc`, `unit_desc`, etc.
6. **Typos** - Parameter names and values must be exact

### Solutions:
1. Start with basic queries
2. Use common commodities (CORN, WHEAT, SOYBEANS)
3. Use major agricultural states
4. Test on NASS website first
5. Check NASS_TROUBLESHOOTING.md for detailed help

## Adding Tested Queries to query_nass.py

Once you've tested a query and confirmed it works:

1. Open `query_nass.py`
2. Find the `EXAMPLE_QUERIES` dictionary
3. Add your query:

```python
EXAMPLE_QUERIES = {
    # ... existing examples ...
    
    9: {
        "name": "Your Query Name",
        "description": "Description of what data this returns",
        "parameters": {
            "commodity_desc": "YOUR_COMMODITY",
            "year": "2022",
            "state_alpha": "XX",
            "statisticcat_desc": "YOUR_STATISTIC"
        }
    }
}
```

## Resources

- **NASS Query Builder**: https://quickstats.nass.usda.gov/
- **API Documentation**: https://quickstats.nass.usda.gov/api
- **Parameter Definitions**: https://quickstats.nass.usda.gov/param_define
- **Get API Key**: https://quickstats.nass.usda.gov/api

## Summary

✅ **Use basic queries** from query_nass.py for reliable results  
⚠️ **Test advanced queries** on NASS website before scripting  
📚 **Reference this file** for advanced query patterns  
🔍 **Start simple** and add complexity gradually  
📖 **Check NASS_TROUBLESHOOTING.md** for detailed debugging  

Remember: Just because a query seems logical doesn't mean the data exists in NASS. Always test first!
