# civdata

Query US environmental compliance data from the terminal — facilities, violations, risk scores, and screening reports.

Covers all 50 states + DC with data from EPA (ECHO, RCRA, CAA, SDWA, SEMS) and 53 state-level environmental agencies.

Dashboard:  https://civdata.dev

## Install

```bash
pip install git+https://github.com/CivData/civdata.git
```

## Usage

```bash
# Search facilities by state, ZIP, county, or name
civdata search --state TX --limit 10
civdata search --county Harris --state TX
civdata search --query "Duke Energy" --format table

# Find facilities near an address or coordinates
civdata nearby "123 Main St, Houston, TX"
civdata nearby "30.27,-97.74" --radius 2.0 --format table

# Get facility detail with violations and risk score
civdata facility epa_echo 110000350174

# Get violations for a facility
civdata violations epa_echo 110000350174 --since 2y

# Environmental screening report for a location
civdata screen "123 Main St, Houston, TX"

# Dataset statistics and available sources
civdata stats
civdata sources
```

## Output formats

All commands support `--format` (`-f`) with three options:

- `json` (default) — full API response
- `table` — human-readable columns
- `csv` — for piping to other tools

```bash
civdata search --state NJ --limit 5 -f table
civdata nearby "Newark, NJ" -f csv > nearby.csv
```

## Data

- ~6.8M facilities across 58 sources
- ~1M violations with type, date, status, and program
- Risk scores (0–100) with confidence levels
- Cross-source entity resolution linking the same facility across EPA and state databases

Data is refreshed daily from federal and state environmental agencies. See `civdata sources` for the full list.

## API

The CLI queries the [CivData API](https://civdata.dev/api/v1/stats). You can point it at a different instance:

```bash
civdata --api-url https://your-instance.example.com stats
```

## License

MIT
