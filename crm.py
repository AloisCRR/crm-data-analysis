import marimo

__generated_with = "0.11.25"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import altair as alt
    import duckdb
    import polars as pl
    return alt, duckdb, pl


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SET autoinstall_known_extensions=true;
        SET autoload_known_extensions=true;
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""# Dataset""")
    return


@app.cell
def _(mo):
    mo.mermaid(
        """
    erDiagram
        ACCOUNTS {
            string account PK "Company name"
            string sector "Industry"
            number year_established "Year Established"
            number revenue "Annual revenue (in millions of USD)"
            number employees "Number of employees"
            string office_location "Headquarters"
            string subsidiary_of FK "Parent company"
        }

        PRODUCTS {
            string product PK "Product name"
            string series "Product series"
            number sales_price "Suggested retail price"
        }

        SALES_TEAMS {
            string sales_agent PK "Sales agent"
            string manager "Respective sales manager"
            string regional_office "Regional office"
        }

        SALES_PIPELINE {
            string opportunity_id PK "Unique identifier"
            string sales_agent FK "Sales agent"
            string product FK "Product name"
            string account FK "Company name"
            string deal_stage "Sales pipeline stage"
            date engage_date "Date in which the 'Engaging' deal stage was initiated"
            date close_date "Date in which the deal was 'Won' or 'Lost'"
            number close_value "Revenue from the deal"
        }

        ACCOUNTS ||--o{ SALES_PIPELINE : "has opportunities"
        PRODUCTS ||--o{ SALES_PIPELINE : "included in"
        SALES_TEAMS ||--o{ SALES_PIPELINE : "manages"
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Accounts""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE TABLE accounts AS SELECT * from '{mo.notebook_location()}/public/accounts.csv'
        """
    )
    return (accounts,)


@app.cell
def _(accounts, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM accounts
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Products""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE TABLE products AS SELECT * from '{mo.notebook_location()}/public/products.csv'
        """
    )
    return (products,)


@app.cell
def _(mo, products):
    _df = mo.sql(
        f"""
        SELECT * FROM products
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Sales pipeline""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE TABLE sales_pipeline AS SELECT * from '{mo.notebook_location()}/public/sales_pipeline.csv'
        """
    )
    return (sales_pipeline,)


@app.cell
def _(mo, sales_pipeline):
    _df = mo.sql(
        f"""
        SELECT * FROM sales_pipeline
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Sales teams""")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE TABLE sales_teams AS SELECT * from '{mo.notebook_location()}/public/sales_teams.csv'
        """
    )
    return (sales_teams,)


@app.cell
def _(mo, sales_teams):
    _df = mo.sql(
        f"""
        SELECT * FROM sales_teams
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Joining the data""")
    return


@app.cell
def _(accounts, mo, products, sales_pipeline, sales_teams):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE VIEW crm_data as SELECT * FROM sales_pipeline sp
            left join sales_teams st on sp.sales_agent = st.sales_agent
            left join products p on sp.product = p.product
            left join accounts a on sp.account = a.account
        """
    )
    return (crm_data,)


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM crm_data
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        # Customer Value Metrics

        Customer Lifetime Value (CLV)

        Basic CLV:

        $$ \text{CLV} = \text{Average Order Value} \times \text{Purchase Frequency} \times \text{Average Customer Lifespan} $$

        Discounted CLV:

        $$ \text{CLV} = \sum_{t=1}^{n} \frac{\text{Annual Profit per Customer}}{(1 + \text{Discount Rate})^t} $$

        Predicted CLV:

        $$ \text{CLV} = \text{Margin} \times \frac{\text{Retention Rate}}{1 + \text{Discount Rate} - \text{Retention Rate}} $$
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Average Order Value""")
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        SELECT 
          AVG(close_value) AS average_order_value
        FROM crm_data
        WHERE deal_stage = 'Won'
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Customer Lifetime Value (CLV)""")
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        WITH customer_value AS (
          SELECT 
            account,
            AVG(close_value) AS avg_value_per_deal,
            COUNT(opportunity_id) AS total_deals,
            DATEDIFF('day', MIN(close_date), MAX(close_date))/365.0 AS customer_lifespan_years
          FROM crm_data
          WHERE deal_stage = 'Won'
          GROUP BY account
        )
        SELECT
          account as company_sold_to,
          avg_value_per_deal,
          total_deals,
          customer_lifespan_years,
          -- Basic CLV calculation
          avg_value_per_deal * total_deals AS historical_clv,
          -- Projected annual CLV (assuming consistent purchase behavior)
          CASE 
            WHEN customer_lifespan_years > 0 
            THEN avg_value_per_deal * (total_deals / customer_lifespan_years) 
            ELSE avg_value_per_deal * total_deals 
          END AS annual_clv,
          -- 3-year projected CLV with 10% discount rate
          CASE 
            WHEN customer_lifespan_years > 0 
            THEN avg_value_per_deal * (total_deals / customer_lifespan_years) * 
                 (1 + 1/(1+0.1) + 1/(1+0.1)^2)
            ELSE avg_value_per_deal * total_deals * 2.74 -- Sum of discount factors
          END AS projected_3yr_clv
        FROM customer_value
        ORDER BY projected_3yr_clv DESC
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        # Customer Acquisition Metrics

        Customer Acquisition Cost (CAC)

        $$ \text{CAC} = \frac{\text{Total Marketing \& Sales Costs}}{\text{Number of New Customers Acquired}} $$

        CAC:CLV Ratio

        $$ \text{CAC:CLV Ratio} = \frac{\text{CLV}}{\text{CAC}} \quad (\text{ideally } 3:1 \text{ or higher}) $$
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Sales Cycle Length as CAC Proxy""")
    return


@app.cell
def _(crm_data, mo):
    sales_cycle_length = mo.sql(
        f"""
        -- Sales Cycle Length as CAC Proxy
        WITH sales_cycle AS (
          SELECT
            account,
            AVG(DATEDIFF('day', engage_date, close_date)) AS avg_sales_cycle_days
          FROM crm_data
          WHERE deal_stage = 'Won'
          GROUP BY account
        ),
        customer_value AS (
          SELECT 
            account,
            AVG(close_value) AS avg_deal_value,
            COUNT(opportunity_id) AS purchase_count,
            SUM(close_value) AS total_value
          FROM crm_data
          WHERE deal_stage = 'Won'
          GROUP BY account
        )
        SELECT
          cv.account,
          sc.avg_sales_cycle_days,
          -- Proxy CAC (assuming $100 per day in marketing/sales costs)
          sc.avg_sales_cycle_days * 100 AS estimated_cac,
          cv.total_value::float AS customer_lifetime_value,
          -- CAC:CLV ratio
          cv.total_value / NULLIF(sc.avg_sales_cycle_days * 100, 0) AS clv_cac_ratio,
          -- CAC Payback Period (in months)
          (sc.avg_sales_cycle_days * 100) / NULLIF(cv.total_value / 12, 0) AS cac_payback_months
        FROM customer_value cv
        JOIN sales_cycle sc ON cv.account = sc.account
        ORDER BY clv_cac_ratio DESC
        """
    )
    return (sales_cycle_length,)


@app.cell
def _(mo):
    mo.md(
        r"""
        # Customer Retention

        Retention Rate

        $$ \text{Retention Rate} = \frac{(\text{Customers at End of Period} - \text{New Customers})}{\text{Customers at Start of Period}} \times 100\% $$

        Churn Rate

        $$ \text{Churn Rate} = \frac{\text{Customers Lost in Period}}{\text{Customers at Start of Period}} \times 100\% $$

        $$ \text{Churn Rate} = 1 - \text{Retention Rate} $$
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Churn Analysis""")
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        WITH customer_activity AS (
          SELECT
            account,
            MIN(close_date) AS first_purchase_date,
            MAX(close_date) AS last_purchase_date,
            DATEDIFF('day', MAX(close_date), '2018-04-15') AS days_since_last_purchase,
            COUNT(opportunity_id) AS purchase_count
          FROM crm_data
          WHERE deal_stage = 'Won'
          GROUP BY account
        )
        SELECT
          account,
          first_purchase_date,
          last_purchase_date,
          days_since_last_purchase,
          purchase_count,
          CASE 
            WHEN days_since_last_purchase > 365 THEN 'Churned'
            WHEN days_since_last_purchase > 180 THEN 'At Risk'
            WHEN days_since_last_purchase > 90 THEN 'Needs Attention'
            ELSE 'Active'
          END AS retention_status
        FROM customer_activity
        ORDER BY days_since_last_purchase DESC
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Repeat Purchase Rate""")
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        SELECT
            account,
            COUNT(opportunity_id) AS purchase_count
        FROM crm_data
        WHERE deal_stage = 'Won'
        GROUP BY account
        """
    )
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        WITH purchase_counts AS (
          SELECT
            account,
            COUNT(opportunity_id) AS purchase_count
          FROM crm_data
          WHERE deal_stage = 'Won'
          GROUP BY account
        )
        SELECT
          SUM(CASE WHEN purchase_count > 1 THEN 1 ELSE 0 END)::float AS repeat_customers,
          COUNT(*) AS total_customers,
          CAST(SUM(CASE WHEN purchase_count > 1 THEN 1 ELSE 0 END) AS FLOAT) / 
            NULLIF(COUNT(*), 0) * 100 AS repeat_purchase_rate
        FROM purchase_counts
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Retention by Cohort""")
    return


@app.cell
def _(crm_data, mo):
    cohort_data = mo.sql(
        f"""
        WITH first_purchases AS (
          SELECT
            account,
            EXTRACT(YEAR FROM MIN(close_date)) AS acquisition_year,
            EXTRACT(MONTH FROM MIN(close_date)) AS acquisition_month
          FROM crm_data
          WHERE deal_stage = 'Won'
          GROUP BY account
        ),
        subsequent_purchases AS (
          SELECT
            cd.account,
            fp.acquisition_year,
            fp.acquisition_month,
            EXTRACT(YEAR FROM cd.close_date) AS purchase_year,
            EXTRACT(MONTH FROM cd.close_date) AS purchase_month,
            DATEDIFF('month', 
              DATE_TRUNC('month', (fp.acquisition_year || '-' || fp.acquisition_month || '-01')::date),
              DATE_TRUNC('month', cd.close_date)) AS months_since_acquisition
          FROM crm_data cd
          JOIN first_purchases fp ON cd.account = fp.account
          WHERE cd.deal_stage = 'Won'
        )
        SELECT
          acquisition_year,
          acquisition_month,
          COUNT(DISTINCT account) AS cohort_size,
          SUM(CASE WHEN months_since_acquisition = 0 THEN 1 ELSE 0 END)::int AS month_0,
          SUM(CASE WHEN months_since_acquisition = 1 THEN 1 ELSE 0 END)::int AS month_1,
          SUM(CASE WHEN months_since_acquisition = 2 THEN 1 ELSE 0 END)::int AS month_2,
          SUM(CASE WHEN months_since_acquisition = 3 THEN 1 ELSE 0 END)::int AS month_3,
          SUM(CASE WHEN months_since_acquisition = 6 THEN 1 ELSE 0 END)::int AS month_6,
          SUM(CASE WHEN months_since_acquisition = 12 THEN 1 ELSE 0 END)::int AS month_12
        FROM subsequent_purchases
        GROUP BY acquisition_year, acquisition_month
        ORDER BY acquisition_year, acquisition_month
        """
    )
    return (cohort_data,)


@app.cell
def _(alt, cohort_data, pl):
    # Step 1: Convert the wide format to long format using unpivot
    _df_melted = cohort_data.unpivot(
        # Columns to unpivot (the month columns)
        on=["month_0", "month_1", "month_2", "month_3", "month_6", "month_12"],
        # Columns to keep as index
        index=["acquisition_year", "acquisition_month", "cohort_size"],
        # Name for the new columns
        variable_name="month",
        value_name="active_customers",
    )

    # Step 2: Clean up the month column and create cohort labels
    _df_melted = _df_melted.with_columns(
        [
            # Remove 'month_' prefix from the month column
            pl.col("month")
            .str.replace("month_", "")
            .cast(pl.Int64)
            .alias("month"),
            # Create cohort label combining year and month
            (
                pl.col("acquisition_year").cast(str)
                + "-"
                + pl.col("acquisition_month").cast(str).str.zfill(2)
            ).alias("cohort"),
        ]
    )

    # Step 3: Calculate retention rate
    _df_melted = _df_melted.with_columns(
        [
            (pl.col("active_customers") / pl.col("cohort_size") * 100).alias(
                "retention_rate"
            )
        ]
    )

    # Step 4: Select and order final columns
    _df_final = _df_melted.select(
        ["cohort", "month", "retention_rate", "active_customers", "cohort_size"]
    ).sort(["cohort", "month"])

    alt.Chart(_df_final).mark_rect().encode(
        x=alt.X("month:O", title="Month Since Acquisition"),
        y=alt.Y("cohort:O", title="Cohort (Acquisition Month)", sort="descending"),
        color=alt.Color(
            "retention_rate:Q",
            scale=alt.Scale(scheme="viridis"),
            title="Retention Rate (%)",
        ),
        tooltip=[
            "cohort",
            "month",
            "retention_rate",
            "active_customers",
            "cohort_size",
        ],
    ).properties(width=600, height=400, title="Customer Retention by Cohort")
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        # Customer Segmentation & RFM Analysis

        RFM Scoring

        - Recency: Time since last purchase

        - Frequency: Number of purchases in a given period

        - Monetary: Total amount spent in a given period
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## RFM Analysis""")
    return


@app.cell
def _(mo):
    rfm = mo.sql(
        f"""
        WITH rfm_data AS (
          SELECT
            account,
            DATEDIFF('day', MAX(close_date), '2018-04-15') AS recency_days,
            COUNT(opportunity_id) AS frequency,
            SUM(close_value) AS monetary
          FROM crm_data
          WHERE deal_stage = 'Won'
          GROUP BY account
        ),
        rfm_scores AS (
          SELECT
            account,
            recency_days,
            frequency,
            monetary,
            NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
            NTILE(5) OVER (ORDER BY frequency) AS f_score,
            NTILE(5) OVER (ORDER BY monetary) AS m_score
          FROM rfm_data
        )
        SELECT
          account,
          recency_days,
          frequency,
          monetary::float as monetary,
          r_score,
          f_score,
          m_score,
          CONCAT(r_score::TEXT, f_score::TEXT, m_score::TEXT) AS rfm_cell,
          -- Customer segmentation logic
          CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 4 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
            WHEN r_score >= 3 AND f_score >= 1 AND m_score >= 2 THEN 'Potential Loyalist'
            WHEN r_score >= 4 AND f_score <= 2 AND m_score <= 2 THEN 'New Customers'
            WHEN r_score >= 3 AND f_score <= 2 AND m_score <= 2 THEN 'Promising'
            WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3 THEN 'At Risk'
            WHEN r_score <= 2 AND f_score >= 3 AND m_score <= 2 THEN 'Needs Attention'
            WHEN r_score <= 2 AND f_score <= 2 AND m_score >= 3 THEN 'Can''t Lose Them'
            WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 'Hibernating'
            ELSE 'About to Sleep'
          END AS customer_segment
        FROM rfm_scores
        ORDER BY 
          CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 1
            WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2 THEN 9
            ELSE 5
          END
        """
    )
    return (rfm,)


@app.cell
def _(alt, rfm):
    alt.Chart(rfm).mark_circle().encode(
        x=alt.X('recency_days:Q', title='Days Since Last Purchase'),
        y=alt.Y('frequency:Q', title='Number of Purchases'),
        size=alt.Size('monetary:Q', title='Total Spend', scale=alt.Scale(range=[50, 500])),
        color=alt.Color('customer_segment:N', title='Customer Segment'),
        tooltip=['account', 'recency_days', 'frequency', 'monetary', 'customer_segment']
    ).properties(
        width=600,
        height=400,
        title='Customer RFM Analysis'
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Customer Value Segmentation""")
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        WITH customer_value AS (
          SELECT
            account,
            sector,
            revenue,
            AVG(close_value) AS avg_deal_size,
            COUNT(opportunity_id) AS purchase_frequency,
            SUM(close_value)::float AS total_spend
          FROM crm_data
          WHERE deal_stage = 'Won'
          GROUP BY account, sector, revenue
        )
        SELECT
          account,
          sector,
          revenue,
          avg_deal_size,
          purchase_frequency,
          total_spend,
          CASE
            WHEN avg_deal_size > (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY avg_deal_size) FROM customer_value)
             AND purchase_frequency > (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY purchase_frequency) FROM customer_value)
            THEN 'High Value (Key Accounts)'

            WHEN avg_deal_size > (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY avg_deal_size) FROM customer_value)
             AND purchase_frequency <= (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY purchase_frequency) FROM customer_value)
            THEN 'Big Spenders (Low Frequency)'

            WHEN avg_deal_size <= (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY avg_deal_size) FROM customer_value)
             AND purchase_frequency > (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY purchase_frequency) FROM customer_value)
            THEN 'Regular Customers (Low Value)'

            ELSE 'Low Value (Growth Opportunity)'
          END AS value_segment
        FROM customer_value
        ORDER BY total_spend DESC
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        # Performance Tracking Metrics

        Conversion Rate

        $$ \text{Conversion Rate} = \frac{\text{Number of Conversions}}{\text{Total Number of Visitors or Leads}} \times 100\% $$

        Sales Cycle Length

        $$ \text{Sales Cycle Length} = \text{Average Time from First Contact to Purchase} $$

        Win Rate

        $$ \text{Win Rate} = \frac{\text{Number of Won Deals}}{\text{Total Number of Deals}} \times 100\% $$
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Deal conversion rates""")
    return


@app.cell
def _(crm_data, mo):
    deal_conversion = mo.sql(
        f"""
        SELECT
          COUNT(*) AS total_opportunities,
          COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) AS won_deals,
          COUNT(CASE WHEN deal_stage = 'Lost' THEN 1 END) AS lost_deals,
          COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS win_rate,
          SUM(CASE WHEN deal_stage = 'Won' THEN close_value ELSE 0 END)::float AS total_revenue,
          AVG(CASE WHEN deal_stage = 'Won' THEN close_value END) AS avg_deal_size
        FROM crm_data
        """
    )
    return (deal_conversion,)


@app.cell
def _(crm_data, mo):
    conversion = mo.sql(
        f"""
        -- Modified query to get counts by stage
        SELECT 
            deal_stage,
            COUNT(*) as count,
            SUM(CASE WHEN deal_stage = 'Won' THEN close_value ELSE 0 END)::float as total_value
        FROM crm_data
        GROUP BY deal_stage
        ORDER BY count DESC
        """
    )
    return (conversion,)


@app.cell
def _(alt, conversion):
    # Make sure deal_stage is treated as a categorical variable
    alt.Chart(conversion).mark_bar().encode(
        x=alt.X('count:Q', title='Number of Opportunities'),
        y=alt.Y('deal_stage:N', 
                sort='-x',  # Sort by count descending
                title='Pipeline Stage'),
        color=alt.Color('deal_stage:N', 
                       scale=alt.Scale(scheme='blues'),
                       legend=None),
        tooltip=['deal_stage:N', 'count:Q', 'total_value:Q']
    ).properties(
        width=500,
        height=300,
        title='Sales Pipeline Conversion'
    )
    return


@app.cell
def _(alt, deal_conversion):
    alt.Chart(deal_conversion).mark_bar().encode(
        x=alt.X('count:Q', title='Number of Opportunities'),
        y=alt.Y('deal_stage:N', 
                sort='-x',  # Sort by count descending
                title='Pipeline Stage'),
        color=alt.Color('deal_stage:N', 
                       scale=alt.Scale(scheme='blues'),
                       legend=None),
        tooltip=['deal_stage', 'count', 'total_value:Q']
    ).properties(
        width=500,
        height=300,
        title='Sales Pipeline Conversion'
    ).transform_window(
        rank='rank(count)',
        sort=[alt.SortField('count', order='descending')]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Sales Cycle Length""")
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        SELECT
          AVG(DATEDIFF('day', engage_date, close_date)) AS avg_sales_cycle_days,
          MIN(DATEDIFF('day', engage_date, close_date)) AS min_sales_cycle_days,
          MAX(DATEDIFF('day', engage_date, close_date)) AS max_sales_cycle_days,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF('day', engage_date, close_date)) AS median_sales_cycle_days
        FROM crm_data
        WHERE deal_stage = 'Won' AND engage_date IS NOT NULL
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Performance by Sales Agent""")
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        SELECT
          sales_agent,
          manager,
          regional_office,
          -- office_location,
          COUNT(opportunity_id) AS total_opportunities,
          COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) AS won_deals,
          COUNT(CASE WHEN deal_stage = 'Lost' THEN 1 END) AS lost_deals,
          COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS win_rate,
          SUM(CASE WHEN deal_stage = 'Won' THEN close_value ELSE 0 END)::float AS total_revenue,
          AVG(CASE WHEN deal_stage = 'Won' THEN close_value END) AS avg_deal_size,
          AVG(CASE WHEN deal_stage = 'Won' THEN DATEDIFF('day', engage_date, close_date) END) AS avg_sales_cycle
        FROM crm_data
        GROUP BY sales_agent, manager, regional_office
        ORDER BY total_revenue DESC;
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        # Product & Channel Performance Metrics

        Product Penetration Rate

        $$ \text{Product Penetration Rate} = \frac{\text{Customers Purchasing Product}}{\text{Total Customers}} \times 100\% $$

        Cross-Sell Rate

        $$ \text{Cross-Sell Rate} = \frac{\text{Number of Customers Buying Multiple Products}}{\text{Total Customers}} \times 100\% $$
        """
    )
    return


@app.cell
def _(mo):
    mo.md("""## Product Performance""")
    return


@app.cell
def _(crm_data, mo):
    product_perf = mo.sql(
        f"""
        SELECT
          product,
          series,
          COUNT(opportunity_id) AS total_opportunities,
          COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) AS won_deals,
          COUNT(CASE WHEN deal_stage = 'Lost' THEN 1 END) AS lost_deals,
          COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS win_rate,
          SUM(CASE WHEN deal_stage = 'Won' THEN close_value ELSE 0 END)::float AS total_revenue,
          AVG(CASE WHEN deal_stage = 'Won' THEN close_value END) AS avg_deal_size,
          AVG(CASE WHEN deal_stage = 'Won' THEN DATEDIFF('day', engage_date, close_date) END) AS avg_sales_cycle,
          COUNT(DISTINCT account) AS unique_customers
        FROM crm_data
        GROUP BY product, series
        ORDER BY total_revenue DESC
        """
    )
    return (product_perf,)


@app.cell
def _(alt, product_perf):
    base = alt.Chart(product_perf).encode(
        x=alt.X("product:N", sort="-y", title="Product")
    )

    # Revenue bars
    bars = base.mark_bar().encode(
        y=alt.Y("total_revenue:Q", title="Total Revenue"),
        color=alt.Color("series:N", title="Product Series"),
        tooltip=[
            "product",
            "total_revenue",
            "win_rate",
            "unique_customers",
            "avg_sales_cycle",
        ],
    )

    # Win rate line
    line = base.mark_line(color="red", point=True).encode(
        y=alt.Y(
            "win_rate:Q", title="Win Rate (%)", axis=alt.Axis(titleColor="red")
        )
    )

    # Combine charts
    alt.layer(bars, line).resolve_scale(y="independent").properties(
        width=700, height=400, title="Product Performance Overview"
    )
    return bars, base, line


@app.cell
def _(mo):
    mo.md(r"""## Product Penetration Rate""")
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        WITH total_customers AS (
          SELECT COUNT(DISTINCT account) AS customer_count
          FROM crm_data
          WHERE deal_stage = 'Won'
        ),
        product_customers AS (
          SELECT
            product,
            COUNT(DISTINCT account) AS customers_with_product
          FROM crm_data
          WHERE deal_stage = 'Won'
          GROUP BY product
        )
        SELECT
          pc.product,
          pc.customers_with_product,
          tc.customer_count AS total_customers,
          pc.customers_with_product * 100.0 / tc.customer_count AS penetration_rate
        FROM product_customers pc
        CROSS JOIN total_customers tc
        ORDER BY penetration_rate DESC
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Cross-Sell Analysis""")
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        WITH customer_products AS (
          SELECT
            account,
            COUNT(DISTINCT product) AS unique_products
          FROM crm_data
          WHERE deal_stage = 'Won'
          GROUP BY account
        ),
        total_customers AS (
          SELECT COUNT(DISTINCT account) AS customer_count
          FROM crm_data
          WHERE deal_stage = 'Won'
        )
        SELECT
          SUM(CASE WHEN unique_products > 1 THEN 1 ELSE 0 END)::float AS cross_sell_customers,
          tc.customer_count AS total_customers,
          SUM(CASE WHEN unique_products > 1 THEN 1 ELSE 0 END) * 100.0 / tc.customer_count AS cross_sell_rate,
          AVG(unique_products) AS avg_products_per_customer,
          MAX(unique_products) AS max_products_per_customer
        FROM customer_products
        CROSS JOIN total_customers tc
        GROUP BY tc.customer_count
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        # Industry & Geographic Analysis

        Industry Penetration Rate

        $$ \text{Industry Penetration Rate} = \frac{\text{Customers in Industry}}{\text{Total Addressable Market in Industry}} \times 100\% $$

        Regional Performance

        $$ \text{Regional Performance Ratio} = \frac{\text{Revenue from Region}}{\text{Total Revenue}} \times 100\% $$
        """
    )
    return


@app.cell
def _(mo):
    mo.md("""## Industry Performance""")
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        SELECT
          sector,
          COUNT(DISTINCT account) AS customer_count,
          COUNT(opportunity_id) AS deal_count,
          COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) AS won_deals,
          COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS win_rate,
          SUM(CASE WHEN deal_stage = 'Won' THEN close_value ELSE 0 END)::float AS total_revenue,
          AVG(CASE WHEN deal_stage = 'Won' THEN close_value END) AS avg_deal_size,
          SUM(CASE WHEN deal_stage = 'Won' THEN close_value ELSE 0 END) / 
            NULLIF(COUNT(DISTINCT CASE WHEN deal_stage = 'Won' THEN account END), 0) AS revenue_per_customer
        FROM crm_data
        GROUP BY sector
        ORDER BY total_revenue DESC
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Geographic Performance""")
    return


@app.cell
def _(crm_data, mo):
    geo_perf = mo.sql(
        f"""
        SELECT
          office_location,
          COUNT(DISTINCT account) AS customer_count,
          COUNT(opportunity_id) AS deal_count,
          COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) AS won_deals,
          COUNT(CASE WHEN deal_stage = 'Lost' THEN 1 END) AS lost_deals,
          COUNT(CASE WHEN deal_stage = 'Won' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS win_rate,
          SUM(CASE WHEN deal_stage = 'Won' THEN close_value ELSE 0 END)::float AS total_revenue,
          AVG(CASE WHEN deal_stage = 'Won' THEN close_value END) AS avg_deal_size
        FROM crm_data
        GROUP BY office_location
        ORDER BY total_revenue DESC
        """
    )
    return (geo_perf,)


@app.cell
def _(alt, geo_perf):
    alt.Chart(geo_perf).mark_bar().encode(
        y=alt.Y('office_location:N', 
                sort='-x',  # Sort by revenue descending
                title='Location'),
        x=alt.X('total_revenue:Q', title='Total Revenue'),
        color=alt.Color('win_rate:Q', 
                       scale=alt.Scale(scheme='viridis'),
                       title='Win Rate (%)'),
        tooltip=['office_location', 'total_revenue', 'win_rate', 
                 'customer_count', 'won_deals', 'lost_deals']
    ).properties(
        width=600,
        height=400,
        title='Revenue by Geographic Location'
    ).interactive()
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        # Predictive Metrics & Leading Indicators

        Customer Health Score

        $$ \text{Health Score} = w_1(\text{Recency}) + w_2(\text{Frequency}) + w_3(\text{Monetary}) + w_4(\text{Engagement}) + w_5(\text{Support}) $$

        where $w_1...w_5$ are weights assigned to each component.

        Churn Probability

        $$ \text{Churn Probability} = f(\text{time since last purchase, purchase frequency, engagement metrics, etc.}) $$
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Customer Health Score (simplified version based on RFM)""")
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        WITH rfm_components AS (
          SELECT
            account,
            -- Recency (inverse: higher is better)
            1.0 / NULLIF(DATEDIFF('day', MAX(close_date), CURRENT_DATE), 0) * 1000 AS recency_score,
            -- Frequency
            COUNT(opportunity_id) AS frequency_score,
            -- Monetary
            SUM(close_value) AS monetary_score
          FROM crm_data
          WHERE deal_stage = 'Won'
          GROUP BY account
        ),
        normalized AS (
          SELECT
            account,
            recency_score / (SELECT MAX(recency_score) FROM rfm_components) * 100 AS norm_recency,
            frequency_score / (SELECT MAX(frequency_score) FROM rfm_components) * 100 AS norm_frequency,
            monetary_score / (SELECT MAX(monetary_score) FROM rfm_components) * 100 AS norm_monetary
          FROM rfm_components
        )
        SELECT
          account,
          norm_recency,
          norm_frequency,
          norm_monetary,
          -- Health score with weights (40% recency, 30% frequency, 30% monetary)
          (0.4 * norm_recency + 0.3 * norm_frequency + 0.3 * norm_monetary) AS health_score,
          CASE
            WHEN (0.4 * norm_recency + 0.3 * norm_frequency + 0.3 * norm_monetary) >= 80 THEN 'Excellent'
            WHEN (0.4 * norm_recency + 0.3 * norm_frequency + 0.3 * norm_monetary) >= 60 THEN 'Good'
            WHEN (0.4 * norm_recency + 0.3 * norm_frequency + 0.3 * norm_monetary) >= 40 THEN 'Average'
            WHEN (0.4 * norm_recency + 0.3 * norm_frequency + 0.3 * norm_monetary) >= 20 THEN 'At Risk'
            ELSE 'Critical'
          END AS health_status
        FROM normalized
        ORDER BY health_score DESC
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Potential Churn Risk Score""")
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        WITH customer_activity AS (
          SELECT
            account,
            MAX(close_date) AS last_purchase_date,
            DATEDIFF('day', MAX(close_date), '2018-04-15') AS days_since_purchase,
            COUNT(opportunity_id) AS purchase_count,
            AVG(DATEDIFF('day', engage_date, close_date)) AS avg_sales_cycle,
            AVG(close_value) AS avg_purchase_value
          FROM crm_data
          WHERE deal_stage = 'Won'
          GROUP BY account
        )
        SELECT
          account,
          last_purchase_date,
          days_since_purchase,
          purchase_count,
          avg_sales_cycle,
          avg_purchase_value,
          -- Churn risk calculation (example formula)
          (days_since_purchase * 0.6) / 
            (NULLIF(purchase_count, 0) * 0.2 + NULLIF(avg_purchase_value, 0) * 0.001 + 1) AS churn_risk_score,
          CASE
            WHEN days_since_purchase > 365 THEN 'Very High Risk'
            WHEN days_since_purchase > 180 THEN 'High Risk'
            WHEN days_since_purchase > 90 THEN 'Medium Risk'
            WHEN days_since_purchase > 30 THEN 'Low Risk'
            ELSE 'Very Low Risk'
          END AS churn_risk_level
        FROM customer_activity
        ORDER BY churn_risk_score DESC
        """
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        # 360° Customer View Metrics

        Customer Engagement Score

        $$ \text{Engagement Score} = w_1(\text{Purchase Frequency}) + w_2(\text{Recency}) + w_3(\text{Interaction Rate}) + w_4(\text{Channel Diversity}) $$

        Customer Profitability Index

        $$ \text{Profitability Index} = \frac{\text{Customer Revenue} - \text{Customer Costs}}{\text{Customer Acquisition Cost}} $$
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## 360° Customer Profile""")
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        SELECT
          cd.account,
          cd.sector,
          cd.year_established,
          cd.revenue AS company_revenue,
          cd.employees,
          cd.office_location,
          COUNT(DISTINCT cd.opportunity_id) AS total_opportunities,
          COUNT(DISTINCT CASE WHEN cd.deal_stage = 'Won' THEN cd.opportunity_id END) AS won_deals,
          SUM(CASE WHEN cd.deal_stage = 'Won' THEN cd.close_value ELSE 0 END)::float AS lifetime_value,
          MIN(CASE WHEN cd.deal_stage = 'Won' THEN cd.close_date END) AS first_purchase_date,
          MAX(CASE WHEN cd.deal_stage = 'Won' THEN cd.close_date END) AS last_purchase_date,
          COUNT(DISTINCT CASE WHEN cd.deal_stage = 'Won' THEN cd.product END) AS unique_products_purchased
        FROM crm_data cd
        GROUP BY cd.account, cd.sector, cd.year_established, cd.revenue, cd.employees, cd.office_location
        ORDER BY lifetime_value DESC
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Customer Touchpoint Analysis""")
    return


@app.cell
def _(crm_data, mo):
    _df = mo.sql(
        f"""
        SELECT
          account,
          AVG(DATEDIFF('day', engage_date, close_date)) AS avg_engagement_to_close_days,
          MIN(DATEDIFF('day', engage_date, close_date)) AS fastest_close_days,
          MAX(DATEDIFF('day', engage_date, close_date)) AS longest_close_days,
          COUNT(DISTINCT sales_agent) AS unique_sales_agents,
          COUNT(DISTINCT product) AS unique_products_considered
        FROM crm_data
        GROUP BY account
        ORDER BY avg_engagement_to_close_days
        """
    )
    return


if __name__ == "__main__":
    app.run()
