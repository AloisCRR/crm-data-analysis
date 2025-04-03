import marimo

__generated_with = "0.11.25"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import duckdb
    import pandas as pd
    return duckdb, pd


@app.cell
def _(duckdb):
    crm_ml_dataset = duckdb.connect("crm_ml_dataset.db")
    return (crm_ml_dataset,)


@app.cell
def _(crm_data, crm_ml_dataset, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM crm_data
        """,
        engine=crm_ml_dataset
    )
    return


@app.cell
def _(accounts, crm_data, crm_ml_dataset, mo):
    features = mo.sql(
        f"""
        WITH account_agg AS
          (SELECT *, completed_deals + failed_deals as total_deals, completed_deals / (completed_deals + failed_deals) as win_rate FROM ( SELECT 
            account, 
            sum(CASE
                   WHEN deal_stage = 'Won' THEN 1
                   ELSE 0
                   END
            )::float AS completed_deals,
           sum(CASE
                   WHEN deal_stage = 'Lost' THEN 1
                   ELSE 0
               END
            )::float AS failed_deals,
            avg(close_date - engage_date) as avg_days_close_deal,
            sum(close_value)::float as total_close_deal_value,
            count(distinct sales_agent) as total_sales_agents
           FROM crm_data
            where deal_stage in ('Won', 'Lost')
           GROUP BY account) i)
        SELECT a.account, sector, 2017 - year_established as company_age, revenue, employees, case when office_location = 'United States' then 'US' else 'Non-US' end as office_location, completed_deals, failed_deals, avg_days_close_deal, total_deals, win_rate
        FROM accounts a
        join account_agg aa on a.account = aa.account
        """,
        engine=crm_ml_dataset
    )
    return (features,)


@app.cell
def _(features):
    features.describe()
    return


@app.cell
def _(features):
    features.to_pandas().info()
    return


@app.cell
def _(features, pd):
    clustering_features = [
        'revenue', 'employees', 'company_age', # Firmographic (numerical)
        'completed_deals', 'failed_deals', 'avg_days_close_deal', 'total_deals', 'win_rate', # Interaction (numerical)
        'sector', 'office_location' # Categorical
    ]
    cluster_data = features.to_pandas()[clustering_features].copy()

    # Store account names
    account_identifiers = features['account']

    # One-Hot Encode
    categorical_cluster_features = ['sector', 'office_location']
    cluster_data_encoded = pd.get_dummies(cluster_data, columns=categorical_cluster_features, drop_first=False)

    X_cluster = cluster_data_encoded
    print("Shape of data for clustering:", X_cluster.shape)
    print("Columns for clustering:", X_cluster.columns.tolist())
    return (
        X_cluster,
        account_identifiers,
        categorical_cluster_features,
        cluster_data,
        cluster_data_encoded,
        clustering_features,
    )


@app.cell
def _(X_cluster):
    from sklearn.preprocessing import StandardScaler
    import matplotlib.pyplot as plt
    import numpy as np

    numerical_cols_for_scaling = X_cluster.select_dtypes(
        include=np.number
    ).columns.tolist()

    X_cluster_transformed = X_cluster.copy()

    features_to_log = [
        "revenue",
        "employees",
    ]

    for col in features_to_log:
        if col in X_cluster_transformed.columns:
            X_cluster_transformed[col] = np.log1p(X_cluster_transformed[col])
            print(f"Applied log1p to {col}")
    return (
        StandardScaler,
        X_cluster_transformed,
        col,
        features_to_log,
        np,
        numerical_cols_for_scaling,
        plt,
    )


@app.cell
def _(StandardScaler, X_cluster_transformed):
    scaler_cluster = StandardScaler()

    # Scale only the numerical columns (original + OHE)
    X_cluster_scaled = scaler_cluster.fit_transform(X_cluster_transformed)

    print("Data transformed (log if applicable) and scaled.")
    return X_cluster_scaled, scaler_cluster


@app.cell
def _(X_cluster_scaled, plt):
    from sklearn.cluster import KMeans

    wcss = []
    k_range = range(1, 11)  # Test K from 1 to 10

    for k in k_range:
        kmeans = KMeans(n_clusters=k, n_init="auto", random_state=42)
        kmeans.fit(X_cluster_scaled)
        wcss.append(kmeans.inertia_)

    plt.figure(figsize=(8, 5))
    plt.plot(k_range, wcss, marker="o", linestyle="--")
    plt.xlabel("Number of Clusters (K)")
    plt.ylabel("WCSS (Inertia)")
    plt.title("Elbow Method for Optimal K")
    plt.xticks(k_range)
    plt.grid(True)
    plt.show()
    return KMeans, k, k_range, kmeans, wcss


@app.cell
def _(KMeans, X_cluster_scaled):
    optimal_k = 4

    kmeans_final = KMeans(n_clusters=optimal_k, n_init="auto", random_state=42)
    kmeans_final.fit(X_cluster_scaled)

    cluster_labels = kmeans_final.labels_
    print(f"K-Means model trained with K={optimal_k}")
    return cluster_labels, kmeans_final, optimal_k


@app.cell
def _(cluster_labels, features, pd):
    _features = features.to_pandas()

    _features['cluster'] = cluster_labels

    print("Cluster Sizes:")
    print(_features['cluster'].value_counts().sort_index())

    # Columns for analysis (include key interaction metrics)
    analysis_num_cols = [
        'revenue', 'employees', 'company_age', # Firmographic
        'completed_deals', 'failed_deals', 'total_deals', 'win_rate', 'avg_days_close_deal' # Interaction
    ]

    print("\nCluster Centers (Mean):")
    pd.set_option('display.float_format', '{:.2f}'.format) # Format output nicely
    print(_features.groupby('cluster')[analysis_num_cols].mean())

    print("\nCluster Centers (Median):")
    print(_features.groupby('cluster')[analysis_num_cols].median())

    print("\nSector Distribution per Cluster:")
    print(pd.crosstab(_features['cluster'], _features['sector']))
    return (analysis_num_cols,)


@app.cell
def _(mo):
    mo.md(
        r"""
        **Cluster 0: "Marketing/Services Engagers" (10 Accounts)**

        *   **Size:** Small group.
        *   **Firmographics:** Tend towards lower-to-medium size (median revenue/employees are lower than C1/C2). Slightly older than C1/C2 (median age 24). Less skew in revenue/employees compared to C1/C2.
        *   **Interaction:** Average total deal volume (median 72.5). Notably, they have the **highest mean and median win rate (0.65)** among the clusters, suggesting interactions are slightly more successful on average. Average close time.
        *   **Sector Focus:** Dominated almost exclusively by **Marketing** (8/10) and **Services** (2/10).
        *   **Persona:** These appear to be smaller or medium-sized, established marketing and service firms. While they don't have the highest interaction *volume*, they seem to be relatively efficient to convert (highest win rate).

        **Cluster 1: "Diverse Mainstream Accounts" (46 Accounts)**

        *   **Size:** The largest group, representing the bulk of the accounts.
        *   **Firmographics:** Broad range in size. The mean revenue/employees are pulled up significantly compared to the median, indicating some large companies are present, but the typical (median) company is medium-sized. Relatively younger (median age 21).
        *   **Interaction:** Average total deal volume (median 74.5). Average win rate (median 0.62). Slightly slower average close time compared to others (mean 49.1, median 49.09).
        *   **Sector Focus:** Highly **Diverse**, covering Retail, Finance, Entertainment, Software, Telecommunications, and Services. This is the most general group.
        *   **Persona:** This is your core, diverse customer base. They span multiple common industries, are typically medium-sized and relatively young, and exhibit average interaction patterns, perhaps taking slightly longer to close deals.

        **Cluster 2: "High-Volume Med/Tech Players" (25 Accounts)**

        *   **Size:** Medium-sized group.
        *   **Firmographics:** Similar profile to Cluster 1 – includes large companies (high mean revenue/employees) but the median suggests a typical medium size. Also relatively young (median age 21).
        *   **Interaction:** Clearly stands out with the **highest interaction volume** (highest mean/median for completed, failed, and total deals). Average win rate (median 0.63). Average close time.
        *   **Sector Focus:** Almost exclusively **Medical** (12/25) and **Technology** (12/25).
        *   **Persona:** These are active medical and technology companies. They generate the most sales activity (both wins and losses). They are prime targets for engagement due to sheer volume, even if their win rate isn't the highest.

        **Cluster 3: "Niche Employment Specialists" (4 Accounts)**

        *   **Size:** Very small, potentially a niche or outlier group.
        *   **Firmographics:** Size profile similar to Cluster 0 (lower-to-medium). Older than C1/C2 (median age 24.5).
        *   **Interaction:** Average total deal volume (median 72.5). Average win rate (median 0.62). Average close time. Interaction patterns are not distinctly different from Cluster 0 or 1 based on these metrics alone.
        *   **Sector Focus:** Exclusively **Employment** (4/4).
        *   **Persona:** A very specific, small group of established companies solely in the employment sector. Their interaction patterns look standard, but their defining feature is their unique industry focus within this dataset.

        **Summary & Actionable Insights:**

        *   The clustering successfully identified distinct groups based largely on **Sector Focus** (Marketing/Services, Med/Tech, Employment, Diverse) and **Interaction Volume** (High volume for Med/Tech).
        *   **Win Rate and Close Time** were less differentiating between the identified clusters in this run.
        *   **Cluster 2 (High-Volume Med/Tech)** likely requires significant sales resources due to the high volume of deals. Strategies could focus on improving efficiency or win rate within this high-activity segment.
        *   **Cluster 0 (Marketing/Services Engagers)** shows good win rates despite potentially smaller size. Nurturing these relationships could be efficient.
        *   **Cluster 1 (Diverse Mainstream)** represents the general customer base. Strategies might involve further sub-segmentation or broad approaches.
        *   **Cluster 3 (Niche Employment)** is very small. Investigate if these 4 accounts have unique needs or if this cluster is too small to be truly representative.
        """
    )
    return


@app.cell
def _(cluster_labels, features, plt, sns):
    features_with_cluster = features.to_pandas()

    features_with_cluster['cluster'] = cluster_labels

    sns.set_style("whitegrid")
    plt.rcParams['figure.dpi'] = 100

    plt.figure(figsize=(8, 5))
    cluster_counts = features_with_cluster['cluster'].value_counts().sort_index()
    sns.barplot(x=cluster_counts.index, y=cluster_counts.values, palette="viridis", hue=cluster_counts.index, legend=False) # Use hue for color mapping
    plt.title('Number of Accounts per Cluster')
    plt.xlabel('Cluster')
    plt.ylabel('Number of Accounts')
    plt.xticks(ticks=cluster_counts.index) # Ensure all cluster numbers are shown

    # Add counts on top of bars
    for index, value in enumerate(cluster_counts):
        plt.text(index, value + 0.5, str(value), ha='center', va='bottom') # Adjust offset as needed

    plt.tight_layout()
    plt.show()
    return cluster_counts, features_with_cluster, index, value


@app.cell
def _(features_with_cluster, plt, sns):
    # features to compare
    features_to_compare = [
        'total_deals',
        'win_rate',
        'avg_days_close_deal',
        'revenue',
        # 'employees'
    ]

    # Decide whether to plot mean or median
    agg_func = 'mean' # or 'median'

    n_features = len(features_to_compare)
    n_cols = 2 # Adjust layout columns as needed
    n_rows = (n_features + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(7 * n_cols, 5 * n_rows))
    axes = axes.flatten() # Flatten axes array for easy iteration

    for i, feature in enumerate(features_to_compare):
        ax = axes[i]
        if agg_func == 'mean':
            data_to_plot = features_with_cluster.groupby('cluster')[feature].mean()
            plot_title = f'Mean {feature.replace("_", " ").title()} per Cluster'
        else:
            data_to_plot = features_with_cluster.groupby('cluster')[feature].median()
            plot_title = f'Median {feature.replace("_", " ").title()} per Cluster'

        sns.barplot(x=data_to_plot.index, y=data_to_plot.values, palette="viridis", hue=data_to_plot.index, legend=False, ax=ax)
        ax.set_title(plot_title)
        ax.set_xlabel('Cluster')
        ax.set_ylabel(f'{agg_func.capitalize()} Value')
        ax.set_xticks(ticks=data_to_plot.index) # Ensure all cluster numbers are shown

    # Hide any unused subplots
    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.tight_layout()
    plt.show()
    return (
        agg_func,
        ax,
        axes,
        data_to_plot,
        feature,
        features_to_compare,
        fig,
        i,
        j,
        n_cols,
        n_features,
        n_rows,
        plot_title,
    )


@app.cell
def _(features_with_cluster, pd, plt):
    sector_crosstab = pd.crosstab(features_with_cluster['cluster'], features_with_cluster['sector'])

    # Normalize to get proportions (optional, but often better for comparison)
    sector_crosstab_norm = sector_crosstab.apply(lambda x: x / x.sum(), axis=1)

    # Plotting (choose either counts or normalized)
    # ax = sector_crosstab.plot(kind='bar', stacked=True, figsize=(12, 7), colormap='tab20')
    _ax = sector_crosstab_norm.plot(kind='bar', stacked=True, figsize=(12, 7), colormap='tab20') # Use tab20 for more distinct colors


    # Add titles and labels
    # plt.title('Sector Count Distribution per Cluster')
    plt.title('Sector Proportion Distribution per Cluster') # If using normalized
    plt.xlabel('Cluster')
    # plt.ylabel('Number of Accounts')
    plt.ylabel('Proportion of Accounts') # If using normalized
    plt.xticks(rotation=0) # Keep cluster numbers horizontal

    # Improve legend placement
    plt.legend(title='Sector', bbox_to_anchor=(1.02, 1), loc='upper left')

    plt.tight_layout(rect=[0, 0, 0.85, 1]) # Adjust layout to make space for legend
    plt.show()
    return sector_crosstab, sector_crosstab_norm


@app.cell
def _(X_cluster_scaled, cluster_labels, pd, plt):
    from sklearn.decomposition import PCA
    import seaborn as sns

    pca = PCA(n_components=2, random_state=42)
    X_cluster_pca = pca.fit_transform(X_cluster_scaled)

    pca_df = pd.DataFrame(data=X_cluster_pca, columns=['PC1', 'PC2'])
    pca_df['cluster'] = cluster_labels

    plt.figure(figsize=(10, 7))
    sns.scatterplot(x='PC1', y='PC2', hue='cluster', palette='viridis', data=pca_df, s=50, alpha=0.7)
    plt.title('Customer Segments (PCA Reduced)')
    plt.xlabel('Principal Component 1')
    plt.ylabel('Principal Component 2')
    plt.legend(title='Cluster')
    plt.grid(True)
    plt.show()

    print(f"Explained variance ratio by PCA components: {pca.explained_variance_ratio_}")
    print(f"Total explained variance: {pca.explained_variance_ratio_.sum():.4f}")
    return PCA, X_cluster_pca, pca, pca_df, sns


if __name__ == "__main__":
    app.run()
