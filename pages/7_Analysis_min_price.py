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
            footer {visibility: hidden;}
            button[kind="header"] {display: none;}
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
    hnp = load_data("./data/tlp.parquet")
    hnp = hnp.with_columns(pl.col("article").cast(pl.Int32),
                          pl.col("year").cast(pl.Int32))

    df_de = (
        df.filter(pl.col("country") == "de")
        .drop("country")
        .with_columns(year=pl.col("date").dt.year())
    )

    st.markdown("## Analysis per e-traders")
    st.divider()

    with st.expander("ℹ️ How to use this page", expanded=False):
        st.markdown(
            """
            **Step 1 – Choose an e-trader**
            - Use the **"Select an e-trader"** dropdown to pick the shop you want to focus on.
            - The analysis will show products where this shop has the **lowest price** (rank 1) and how much cheaper it is compared to competitors.

            **Step 2 – Set a minimum price difference**
            - Use the **"Minimum price difference in Euro"** slider to filter out small differences.
            - Only products where at least one competitor charges **at least this much more** than the selected shop will appear.

            **Step 3 – Choose how many competitor ranks to show**
            - Use the **"Ranks to display"** dropdown to control how many competing shops are shown per product (1 = only the next cheapest, 2 = next two, etc.).

            **Step 4 – Toggle delivery costs**
            - Tick **"Show for prices with delivery"** to include shipping costs in all price comparisons.

            **Step 5 – Select a reference date**
            - Use the **"Select a date"** picker to set the date for the analysis.

            ---

            **Reading the results table**
            | Column | Meaning |
            |---|---|
            article / product | Product identifier and name |
            *Selected shop* | Price of the selected e-trader (always the first shop column) |
            shop | Competitor shop name |
            price | Competitor's price on the reference date |
            diff | Price difference: competitor price minus the selected shop's price (always positive — selected shop is cheaper) |

            Rows are sorted by the **largest maximum difference** first, so the products where the selected shop has the biggest price advantage appear at the top.
            """
        )

    col1, col2, col3, col4, col5 = st.columns(5, gap="medium")
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
        rank = st.selectbox(
            "Ranks to display",
            options=[1, 2, 3, 4],
            index=0,
        )
    with col4:
        disc = st.checkbox("Show for prices with delivery", value=False)
    with col5:
        date1 = st.date_input(
            "Select a date",
            df_de["date"].max(),
            key="date_range1",
        )

    df_de = (
        df_de.filter(pl.col("date") == date1)
        .join(
            hnp.select(pl.col("article", "year", "price", "product")),
            on=["article", "year"],
            how="left",
            # coalesce=True,
        )
        .with_columns(
            disc1=1 - pl.col("price") / pl.col("price_right"),
            disc2=1 - pl.col("price_delivery") / pl.col("price_right"),
        )
    )
    column2 = "price_delivery" if disc else "price"

    df_de_sorted = df_de.sort(["date", column2]).with_columns(
        pl.col(column2).rank("min").over(["date", "article"]).alias("rank")
    )

    df_de_sorted = df_de.sort(["date", column2]).with_columns(
        pl.col(column2).rank("min").over(["date", "article"]).alias("rank")
    )

    art = (
        df_de_sorted.filter(pl.col("shop") == shop1, pl.col("rank") == 1)
        .get_column("article")
        .to_list()
    )
    df_de_sorted2 = (
        df_de_sorted.filter(pl.col("article").is_in(art), pl.col("rank") <= rank + 1)
        .select(["article", "shop", "product", column2])
        .sort(by=["article", "shop"], descending=[True, False], nulls_last=True)
    )

    df_de_sorted2_pivot = df_de_sorted2.pivot(
        values=column2,
        index=["article", "product"],
        on="shop",
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

    df_unp = (
        df_de_sorted22_pivot.unpivot(
            index=["article", "product", f"{shop1}"],
            value_name="price",
            variable_name="shop",
        )
        .filter(pl.col("price").is_not_null())
        .with_columns((pl.col("price") - pl.col(f"{shop1}")).alias("diff"))
    )
    df_unp2 = df_unp.with_columns(
        prmax=pl.col("diff").max().over(["article"]),
    ).filter(pl.col("diff") >= min_diff)
    df_unpivoted = (
        df_unp.join(df_unp2, "article", how="left")
        .filter(pl.col("prmax").is_not_null())
        .sort(["prmax", "diff"], descending=[True, False])
        .select(["article", "product", f"{shop1}", "shop", "price", "diff"])
        .unique(subset=["article", "shop"], keep="first", maintain_order=True)
    )
    df_unpivoted_rend = df_unpivoted.with_columns(
        pl.col("article").map_elements(
            lambda x: "{:,}".format(x).replace(",", ""),
            skip_nulls=False,
            return_dtype=pl.Utf8,
        ),
    )

    st.divider()
    try:
        st.dataframe(df_unpivoted_rend, hide_index=True, width='stretch')
    except:
        st.write("No data to display")
