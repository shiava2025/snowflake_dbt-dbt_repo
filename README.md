# Dev Analytics DBT Project

A comprehensive data transformation pipeline for software development analytics, tracking engineering productivity, code quality, and delivery metrics.
This project uses custom quality checks to find patterns, anomolies and trends along with sample source data that can be loaded with seed. 
## 📌 Key Features

- **Developer Productivity Metrics**: Velocity, throughput, and impact scores
- **Code Health Indicators**: Churn rate, complexity, and technical debt ratios
- **Delivery Performance**: Lead time, deployment frequency, change failure rate
- **Anomaly Detection**: Automated statistical alerts for unusual patterns
- **Trend Analysis**: Rolling windows and cohort-based metrics

## 🛠️ Technical Stack

| Component       | Technology               |
|-----------------|--------------------------|
| Data Warehouse | Snowflake                 |
| Orchestration  | Airflow                   |
| CI/CD          | GitHub Actions            |

## 🗂 Project Structure

```bash
.
├── models/
│   ├── staging/    # Source-aligned raw data transformations
│   ├── intermediate/ # Business logic transformations
│   └── marts/      # Domain-aligned analytics ready models
├── snapshots/      # Slowly-changing dimensions
├── macros/         # Reusable Jinja components
├── tests/          # Custom data tests
└── seeds/          # Reference data
```

## 🚀 Getting Started

### Prerequisites
- Python 3.10.8
- DBT Core 1.10.5


### Installation
```bash
# Clone repository
git clone https://github.com/yourorg/dev-analytics-dbt.git

# Install dependencies
pip install -r requirements.txt
dbt deps

# Configure profiles.yml
cp profiles.example.yml ~/.dbt/profiles.yml
```

### Running the Project

```bash
# Full pipeline run
dbt build

# Run specific models with tags
dbt run --select tag:engineering

# Generate documentation
dbt docs generate
dbt docs serve
```


## 🛑 Alert Conditions

The project includes automated alerts for:

1. **Velocity Drops**: When developer scores fall below threshold:
   ```sql
   {{ config(severity = 'warn' if var('velocity_score_warning') else 'error' )}}
   ```

2. **Churn Spikes**: Statistical anomalies in code churn
3. **Review Bottlenecks**: PRs aging beyond SLA
4. **Incident Trends**: Rising MTTR patterns

## 🤝 Contribution Guidelines

1. Branch naming: `feature/[name]` or `fix/[issue]`
2. All models require:
   - YAML documentation
   - Tests for critical columns
   - Incremental logic where applicable
3. Use tags for model categorization:
   ```jinja
   {{ config(tags=['engineering', 'metrics'])}}
   ```

## 📊 Example Dashboard Queries

```sql
-- Weekly Team Performance
select 
  team_name,
  avg(velocity_score) as avg_velocity,
  avg(complexity_score) as avg_complexity
from {{ ref('fct_engineering_metrics') }}
group by 1
```

## 📅 Release Schedule

- **Nightly**: Full incremental refresh
- **Weekly**: Snapshots and historical trends
- **Monthly**: Metric recalculations with 90-day lookback

## 🔒 Data Governance

- PII: Developer emails are hashed in staging
- Retention: Raw data kept for 365 days
- Access: Role-based permissions in warehouse
