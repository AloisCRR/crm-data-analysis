import marimo

__generated_with = "0.11.25"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import pandas as pd
    import random

    df = pd.DataFrame(
        {
            "category": [random.choice(["A", "B", "C"]) for _ in range(20)],
            "value": list(range(20)),
        }
    )
    return df, pd, random


@app.cell
def _(df, mo):
    _df = mo.sql(
        f"""
        SELECT category, MEAN(value) as mean FROM df GROUP BY category ORDER BY mean;
        """
    )
    return


if __name__ == "__main__":
    app.run()
