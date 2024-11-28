import polars as pl
import streamlit as st
import plotly.graph_objects as go
from middleware import authenticate_user
from datetime import date, timedelta
import toml
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import io

config = toml.load("./.streamlit/secrets.toml")
key = config["secrets"]["data_key"].encode("utf-8")


def decrypt_data(data, key):
    cipher = AES.new(key, AES.MODE_CBC, iv=data[:16])
    pt = unpad(cipher.decrypt(data[16:]), AES.block_size)
    return pt


# Page configuration
st.set_page_config(
    page_title="E-trader analysis", layout="wide", initial_sidebar_state="expanded"
)


# Change the font of the entire app
def set_font(font):
    st.markdown(
        f"""
                <style>
                body {{font-family: {font};}}
                </style>
                """,
        unsafe_allow_html=True,
    )


set_font("Arial")

# --- HIDE STREAMLIT STYLE ---
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)

if authenticate_user():

    @st.cache_data
    def load_data(path):
        with open(path, "rb") as f:
            encrypted_data = f.read()
            buffer = io.BytesIO(decrypt_data(encrypted_data, key))
            df = pl.read_parquet(buffer)
        return df

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

    df = load_data("./data/Ien.parquet")
    hnp = load_data("./data/hnp24.parquet")
    hnp.columns = [col.lower() for col in hnp.columns]
    hnp = hnp.with_columns(
        pl.col("article").cast(pl.Int32),
    )

    df_de = (
        df.filter(pl.col("country") == "de")
        .join(
            hnp.select(pl.col("article", "hnp", "subcategory", "family", "product")),
            on="article",
            how="left",
            # coalesce=True,
        )
        .drop("country")
        .with_columns(
            disc1=1 - pl.col("price") / pl.col("hnp"),
            disc2=1 - pl.col("price_delivery") / pl.col("hnp"),
        )
    )
    subcat = df_de["subcategory"].unique().sort().to_list()

    st.markdown("## Analysis per e-traders")
    st.divider()

    col1, col2, col3, col4 = st.columns(4, gap="medium")
    with col1:
        shop1 = st.selectbox(
            "Select an e-trader", df_de["shop"].unique().sort().to_list(), index=1
        )
    with col2:
        min_diff = st.slider(
            "Minimum price difference in Euro",
            min_value=1.0,
            max_value=30.0,
            value=10.0,
            step=1.0,
        )
    with col3:
        disc = st.checkbox("Show for prices with delivery", value=False)
    with col4:
        date1 = st.date_input(
            "Select a date",
            df_de["date"].max(),
            key="date_range1",
        )

    df_de = df_de.filter(pl.col("date") == date1)
    column2 = "price_delivery" if disc else "price"

    df_de_sorted = df_de.sort(["date", column2]).with_columns(
        pl.col(column2).rank("min").over(["date", "article"]).alias("rank")
    )

    df_de_sorted = df_de.sort(["date", column2]).with_columns(
        pl.col(column2).rank("min").over(["date", "article"]).alias("rank")
    )

    art = df_de_sorted.filter(pl.col("shop") == shop1, pl.col("rank") == 1).select(
        "article"
    )
    df_de_sorted2 = (
        df_de_sorted.filter(pl.col("article").is_in(art), pl.col("rank") < 3)
        .select(["article", "shop", "product", column2])
        .sort(by=["article", "shop"], descending=[True, False], nulls_last=True)
    )

    df_de_sorted2_pivot = df_de_sorted2.pivot(
        values=column2,
        index=["article", "product"],
        columns="shop",
        aggregate_function="first",
    )

    columns = (
        df_de_sorted2_pivot.columns
    )  # work with columns of pivoted table to fix the order
    fixed_columns = ["article", "product"]
    shop_columns = [col for col in columns if col not in fixed_columns]
    try:
        shop_columns.remove(f"{shop1}")
        shop_columns.insert(0, f"{shop1}")
    except:
        pass
    new_columns_order = fixed_columns + shop_columns
    df_de_sorted22_pivot = df_de_sorted2_pivot.select(new_columns_order)

    df_unpivoted = (
        (
            df_de_sorted2_pivot.melt(
                id_vars=["article", "product", f"{shop1}"],
                value_name="price",
                variable_name="shop",
            )
            .filter(pl.col("price").is_not_null())
            .with_columns((pl.col("price") - pl.col(f"{shop1}")).alias("diff"))
        )
        .filter(pl.col("diff") > min_diff)
        .sort("diff", descending=True)
    )
    df_unpivoted_rend = df_unpivoted.with_columns(
        pl.col("article").map_elements(lambda x: "{:,}".format(x).replace(",", ""))
    )

    st.divider()
    try:
        st.dataframe(df_unpivoted_rend, hide_index=True, use_container_width=True)
    except:
        st.write("No data to display")
