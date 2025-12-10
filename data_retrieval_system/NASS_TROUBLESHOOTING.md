# USDA NASS Query Troubleshooting

## Common Query Errors and Solutions

### Error: "bad request - invalid query"

This error occurs when the combination of parameters doesn't exist in the USDA NASS database.

**Common Causes:**

1. **Invalid Commodity Name**
   - Must match EXACTLY as stored in NASS
   - Examples: `CORN`, `WHEAT`, `SOYBEANS` (not `SOYBEAN`)
   - Use all caps

2. **Invalid Parameter Combinations**
   - Not all commodities have all statistics
   - Not all commodities are tracked in all states/years
   - Some classifications don't exist for all commodities

3. **Invalid Class Description**
   - `ORGANIC` data may not exist for all commodities/years
   - Check if organic data is available for that commodity

## Validated Working Queries

These queries are tested and known to work:

### 1. Corn Production by State
```python
{
    "commodity_desc": "CORN",
    "year": "2022",
    "state_alpha": "IA",
    "statisticcat_desc": "PRODUCTION"
}
```

### 2. Wheat Yield
```python
{
    "commodity_desc": "WHEAT",
    "year": "2022",
    "state_alpha": "KS",
    "statisticcat_desc": "YIELD"
}
```

### 3. Soybean Acres Planted
```python
{
    "commodity_desc": "SOYBEANS",
    "year": "2022",
    "state_alpha": "IL",
    "statisticcat_desc": "AREA PLANTED"
}
```

### 4. Cattle Inventory
```python
{
    "commodity_desc": "CATTLE",
    "year": "2022",
    "state_alpha": "TX",
    "statisticcat_desc": "INVENTORY"
}
```

### 5. Milk Production
```python
{
    "commodity_desc": "MILK",
    "year": "2022",
    "state_alpha": "WI",
    "statisticcat_desc": "PRODUCTION"
}
```

### 6. Organic Corn Production
```python
{
    "commodity_desc": "CORN",
    "class_desc": "ORGANIC",
    "year": "2021",
    "statisticcat_desc": "PRODUCTION",
    "agg_level_desc": "STATE"
}
```

## Troubleshooting Steps

### Step 1: Start Simple
Begin with basic parameters and add complexity:

```python
# Start with this
{
    "commodity_desc": "CORN",
    "year": "2022"
}

# Then add state
{
    "commodity_desc": "CORN",
    "year": "2022",
    "state_alpha": "IA"
}

# Then add statistic
{
    "commodity_desc": "CORN",
    "year": "2022",
    "state_alpha": "IA",
    "statisticcat_desc": "PRODUCTION"
}
```

### Step 2: Verify Parameter Values

**Check Commodity Names:**
Common commodities:
- `CORN` ✓
- `WHEAT` ✓
- `SOYBEANS` ✓ (not SOYBEAN)
- `COTTON` ✓
- `CATTLE` ✓
- `HOGS` ✓
- `MILK` ✓
- `BARLEY` ✓
- `OATS` ✓
- `RICE` ✓

**Check Statistic Categories:**
- `PRODUCTION` - Total production
- `YIELD` - Production per unit area
- `AREA PLANTED` - Acres planted
- `AREA HARVESTED` - Acres harvested
- `INVENTORY` - Livestock inventory
- `PRICE RECEIVED` - Price data

**Check State Codes:**
- Must be 2-letter uppercase state abbreviations
- Examples: `IA`, `IL`, `TX`, `CA`, `WI`

### Step 3: Check Data Availability

Not all data exists for all combinations:

❌ **Unlikely to work:**
```python
{
    "commodity_desc": "CORN",
    "state_alpha": "HI",  # Hawaii doesn't grow much corn
    "statisticcat_desc": "PRODUCTION"
}
```

✓ **More likely to work:**
```python
{
    "commodity_desc": "CORN",
    "state_alpha": "IA",  # Iowa is top corn state
    "statisticcat_desc": "PRODUCTION"
}
```

### Step 4: Use Aggregation Levels

When querying organic or specialized data, specify aggregation:

```python
{
    "commodity_desc": "CORN",
    "class_desc": "ORGANIC",
    "statisticcat_desc": "PRODUCTION",
    "year": "2021",
    "agg_level_desc": "STATE"  # Important for organic queries
}
```

### Step 5: Try Different Years

Some data may not be available for recent years:

```python
# If 2023 doesn't work, try 2022
{
    "commodity_desc": "CORN",
    "year": "2022",  # Use previous year
    "state_alpha": "IA"
}
```

## Common Issues by Query Type

### Organic Queries

**Problem:** Organic data is limited

**Solution:**
```python
{
    "commodity_desc": "CORN",  # or SOYBEANS
    "class_desc": "ORGANIC",
    "year": "2021",  # Use 2021 or earlier
    "statisticcat_desc": "PRODUCTION",
    "agg_level_desc": "STATE"  # Must specify aggregation
}
```

### County-Level Queries

**Problem:** Not all data available at county level

**Solution:**
```python
{
    "commodity_desc": "CORN",
    "state_alpha": "IA",
    "county_name": "STORY",  # County name in caps
    "year": "2022",
    "statisticcat_desc": "PRODUCTION",
    "agg_level_desc": "COUNTY"  # Explicitly set county level
}
```

### Price Queries

**Problem:** Price data uses different parameters

**Solution:**
```python
{
    "commodity_desc": "CORN",
    "year": "2022",
    "statisticcat_desc": "PRICE RECEIVED",
    "agg_level_desc": "STATE",
    "state_alpha": "IA"
}
```

### Vegetable/Fruit Queries

**Problem:** Vegetable names can be tricky

**Solution:**
Use specific vegetable names:
```python
{
    "commodity_desc": "LETTUCE",  # Specific vegetable
    "year": "2022",
    "state_alpha": "CA",
    "statisticcat_desc": "PRODUCTION"
}
```

Don't use:
- "VEGETABLE TOTALS" ❌
- "VEGETABLES" ❌

## Testing Queries

### Using the NASS Website

Before running queries in the script, test them on the NASS website:

1. Go to: https://quickstats.nass.usda.gov/
2. Use the dropdown menus to select parameters
3. Click "Get Data"
4. If data appears, the query will work in the script

### Using the Script

Test queries one parameter at a time:

```bash
# Test basic query first
python query_nass.py --custom

# Enter simple query
{
    "commodity_desc": "CORN",
    "year": "2022"
}

# If that works, add more parameters
{
    "commodity_desc": "CORN",
    "year": "2022",
    "state_alpha": "IA"
}
```

## Error Messages and Fixes

### "bad request - invalid query"
**Fix:** Simplify query, check commodity name spelling, verify data exists

### "Connector not found"
**Fix:** Run `python add_connectors.py usda_quickstats`

### "Invalid API key"
**Fix:** Update connector with valid API key

### "Rate limit exceeded"
**Fix:** Wait a few minutes, queries are cached automatically

### "No data returned"
**Fix:** Data may not exist for that combination, try different parameters

## Best Practices

1. **Start Broad, Then Narrow**
   - Begin with just commodity and year
   - Add state, then statistic, then additional filters

2. **Use Common Commodities First**
   - CORN, WHEAT, SOYBEANS are widely tracked
   - Try these before specialty crops

3. **Check Recent Years**
   - Use 2021-2022 for most queries
   - Some data has reporting delays

4. **Specify Aggregation for Special Queries**
   - Always use `agg_level_desc` for organic queries
   - Use it for county-level queries

5. **Test on NASS Website First**
   - Validate parameter combinations
   - Verify data availability

## Example: Fixing a Failed Query

**Failed Query:**
```python
{
    "class_desc": "ORGANIC",
    "commodity_desc": "VEGETABLE TOTALS",
    "statisticcat_desc": "PRODUCTION",
    "year": "2021"
}
```
**Error:** "bad request - invalid query"

**Fixed Query:**
```python
{
    "commodity_desc": "CORN",  # Use specific commodity
    "class_desc": "ORGANIC",
    "statisticcat_desc": "PRODUCTION",
    "year": "2021",
    "agg_level_desc": "STATE"  # Add aggregation level
}
```

## Resources

- **NASS Query Builder**: https://quickstats.nass.usda.gov/
- **Parameter Definitions**: https://quickstats.nass.usda.gov/param_define
- **API Documentation**: https://quickstats.nass.usda.gov/api
- **Get API Key**: https://quickstats.nass.usda.gov/api

## Quick Reference: Working Query Templates

### Crop Production
```python
{
    "commodity_desc": "CORN",
    "year": "2022",
    "state_alpha": "IA",
    "statisticcat_desc": "PRODUCTION"
}
```

### Crop Yield
```python
{
    "commodity_desc": "WHEAT",
    "year": "2022",
    "state_alpha": "KS",
    "statisticcat_desc": "YIELD"
}
```

### Acres Planted/Harvested
```python
{
    "commodity_desc": "SOYBEANS",
    "year": "2022",
    "state_alpha": "IL",
    "statisticcat_desc": "AREA PLANTED"
}
```

### Livestock Inventory
```python
{
    "commodity_desc": "CATTLE",
    "year": "2022",
    "state_alpha": "TX",
    "statisticcat_desc": "INVENTORY"
}
```

### Organic Production
```python
{
    "commodity_desc": "CORN",
    "class_desc": "ORGANIC",
    "year": "2021",
    "statisticcat_desc": "PRODUCTION",
    "agg_level_desc": "STATE"
}
```

These templates have been tested and work reliably!
