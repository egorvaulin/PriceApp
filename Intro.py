import logging
import streamlit as st
import polars as pl
from datetime import timedelta
from middleware import authenticate_user
from utils import get_key, load_parquet, apply_styling, data_path

logger = logging.getLogger(__name__)

key = get_key()


# Page configuration
st.set_page_config(
    page_title="Price monitoring dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


apply_styling()

if authenticate_user():
    st.markdown("## Price monitoring dashboard")
    # Launch intro page
    st.divider()
    st.markdown(
        "##### This dashboard is created to monitor the prices on Idealo. The data is scraped from the website and updated every 24 hours."
    )

    try:
        df = load_parquet(data_path("Ien.parquet"), key)
    except Exception:
        st.error("Failed to load data. Please contact the administrator.")
        st.stop()

    df_de = df.filter(pl.col("country") == "de").with_columns(
        pl.col("date").cast(pl.Date)
    )
    df_de_ld = df_de.filter(
        pl.col("date") >= (pl.col("date").max() - timedelta(days=10))
    )

    df_de_ld_grouped = (
        (
            df_de_ld.drop("country", "price_delivery")
            .sort("price")
            .group_by("date", "article")
            .agg(pl.col("shop").head(3))
            .explode("shop")
        )["shop"]
        .value_counts()
        .sort("count", descending=True)
        .head(5)
    )
    top_de_shops = ", ".join(df_de_ld_grouped["shop"].to_list())

    df_de_ld_grouped2 = (
        (
            df_de_ld.drop("country", "price")
            .sort("price_delivery")
            .group_by("date", "article")
            .agg(pl.col("shop").head(3))
            .explode("shop")
        )["shop"]
        .value_counts()
        .sort("count", descending=True)
        .head(5)
    )
    top_de_shops2 = ", ".join(df_de_ld_grouped2["shop"].to_list())

    last_date_de = df["date"].max().strftime("%d.%m.%Y")
    shops_de = df_de["shop"].unique()
    shops_de_num = shops_de.len()

    def custom_metric(label, value):
        st.markdown(
            f"""
            <div style="border:1px solid #343499; border-left:8px solid #343499; border-radius:2px; padding:10px; margin:5px; text-align:center; height:180px; overflow:auto;">
                <h5>{label}</h5>
                <h2>{value}</h2>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # Use the custom metric function
    col1, col2 = st.columns(2)
    with col1:
        custom_metric("Last date in the price monitor", last_date_de)
        custom_metric(
            "Shops with lowest prices among all products",
            f"<span style='font-size: small;'>{top_de_shops}</span>",
        )

    with col2:
        custom_metric("Quantity of e-traders", shops_de_num)
        custom_metric(
            "Shops with lowest prices with delivery",
            f"<span style='font-size: small;'>{top_de_shops2}</span>",
        )
    st.divider()

    st.dataframe(shops_de.sort().to_frame(), hide_index=True)
