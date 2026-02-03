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
    page_title="E-trader analysis", layout="wide", initial_sidebar_state="collapsed"
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
    hnp = load_data("./data/tlp.parquet")
    hnp = hnp.with_columns(pl.col("article").cast(pl.Int32))

    df_de = (
        df.filter(pl.col("country") == "de")
        .drop("country")
        .with_columns(year=pl.col("date").dt.year())
    )

    st.markdown("## Analysis per e-traders")
    st.divider()

    col1, col2, col3, col4 = st.columns(4, gap="medium")
    with col1:
        shop1 = st.selectbox(
            "Select an e-trader", df_de["shop"].unique().sort().to_list(), index=0
        )
    with col2:
        shop2 = st.selectbox(
            "Select an e-trader", df_de["shop"].unique().sort().to_list(), index=1
        )
    with col3:
        disc = st.checkbox("Show for prices with delivery", value=False)
    with col4:
        date1 = st.date_input(
            "Select a date",
            df_de["date"].max(),
            key="date_range1",
        )

    df_de_shop = (
        df_de.filter(pl.col("shop").is_in([shop1, shop2]))
        .join(
            hnp.select(pl.col("article", "year", "price", "family", "product")),
            on=["article", "year"],
            how="left",
            # coalesce=True,
        )
        .with_columns(
            disc1=1 - pl.col("price") / pl.col("price_right"),
            disc2=1 - pl.col("price_delivery") / pl.col("price_right"),
        )
    )
    df_de = (
        df_de.filter(pl.col("date") == date1)
        .join(
            hnp.select(pl.col("article", "year", "price", "family", "product")),
            on=["article", "year"],
            how="left",
            # coalesce=True,
        )
        .with_columns(
            disc1=1 - pl.col("price") / pl.col("price_right"),
            disc2=1 - pl.col("price_delivery") / pl.col("price_right"),
        )
    )

    last_day = date1
    previous_day = date1 - timedelta(days=1)
    previous_week = date1 - timedelta(weeks=1)
    previous_month = date1 - timedelta(days=30)
    dates = [previous_month, previous_week, previous_day, last_day]

    df_de_show = df_de_shop.filter(pl.col("date").is_in(dates))
    df_de_disc = df_de_shop.filter(pl.col("date") == date1)
    column2 = "price_delivery" if disc else "price"

    def price_changes(shop):
        pivot_df = (
            df_de_show.filter(pl.col("shop") == shop)
            .with_columns(pl.col("date").cast(pl.Utf8))
            .pivot(
                values=column2,
                index="product",
                on="date",
                aggregate_function="first",
            )
        )

        # Ensure there are enough columns to perform the analysis
        if len(pivot_df.columns) < 5:
            # Not enough columns for month/week/day/last comparisons
            return [0, 0, 0, 0, 0, 0, pivot_df]

        last_column_name = pivot_df.columns[-1]

        for col_name in pivot_df.columns[1:-1]:  # Exclude the first ('product') and the last column
            pivot_df = pivot_df.with_columns(
                (pl.col(col_name) - pl.col(last_column_name)).alias(col_name)
            )

        day_ago_positive = pivot_df.filter(
            pl.col(pivot_df.columns[-2]) > 0
        ).height  # Count positive and negative values in 'day ago' column
        day_ago_negative = pivot_df.filter(pl.col(pivot_df.columns[-2]) < 0).height

        week_ago_positive = pivot_df.filter(
            pl.col(pivot_df.columns[-3]) > 0
        ).height  # Count positive and negative values in 'week ago' column
        week_ago_negative = pivot_df.filter(pl.col(pivot_df.columns[-3]) < 0).height

        month_ago_positive = pivot_df.filter(
            pl.col(pivot_df.columns[-4]) > 0
        ).height  # Count positive and negative values in 'month ago' column
        month_ago_negative = pivot_df.filter(pl.col(pivot_df.columns[-4]) < 0).height
        return [
            day_ago_positive,
            day_ago_negative,
            week_ago_positive,
            week_ago_negative,
            month_ago_positive,
            month_ago_negative,
            pivot_df,
        ]

    column = "disc1" if not disc else "disc2"
    df_de_sorted = df_de.sort(["date", column2]).with_columns(
        pl.col(column2).rank("min").over(["date", "article"]).alias("rank")
    )
    shop_rank_counts = df_de_sorted.group_by(["shop", "rank"]).len()
    shop_rank_counts_1 = (
        shop_rank_counts.filter(pl.col("shop") == shop1, pl.col("rank").is_not_null())
        .sort("rank")
        .with_columns(
            pl.col("len").alias("count"),
        )
        .select("shop", "rank", "count")
        .head(4)
    )  # Filter the DataFrame for the first shop
    shop_rank_counts_2 = (
        shop_rank_counts.filter(pl.col("shop") == shop2, pl.col("rank").is_not_null())
        .sort("rank")
        .with_columns(
            pl.col("len").alias("count"),
        )
        .select("shop", "rank", "count")
        .head(4)
    )  # Filter the DataFrame for the second shop

    df_de_sorted1_ranked = (
        df_de_sorted.filter(pl.col("shop") == shop1, pl.col("rank") == 1)
        .select(["article", "product", column2, column])
        .sort(by=[column, "product"], descending=[True, False], nulls_last=True)
    )
    df_de_sorted12_ranked = (
        df_de_sorted.filter(pl.col("shop") == shop1, pl.col("rank") == 2)
        .select(["article", "product", column2, column])
        .sort(by=[column, "product"], descending=[True, False], nulls_last=True)
    )

    df_de_sorted2_ranked = (
        df_de_sorted.filter(pl.col("shop") == shop2, pl.col("rank") == 1)
        .select(["article", "product", column2, column])
        .sort(by=[column, "product"], descending=[True, False], nulls_last=True)
    )
    df_de_sorted22_ranked = (
        df_de_sorted.filter(pl.col("shop") == shop2, pl.col("rank") == 2)
        .select(["article", "product", column2, column])
        .sort(by=[column, "product"], descending=[True, False], nulls_last=True)
    )

    coln1, coln2, coln3 = st.columns([2, 4, 4], gap="large")
    with coln1:
        st.write(f"Rank counts for {shop1}")
        st.dataframe(shop_rank_counts_1, hide_index=True, width='stretch')
        st.write(f"Rank counts for {shop2}")
        st.dataframe(shop_rank_counts_2, hide_index=True, width='stretch')

    with coln2:
        df_de_sorted1_ranked = df_de_sorted1_ranked.with_columns(
            pl.col("article").map_elements(
                lambda x: "{:,}".format(x).replace(",", ""),
                skip_nulls=False,
                return_dtype=pl.Utf8,
            ),
            pl.col(column2).map_elements(
                lambda x: "{:,}".format(x).replace(".", ","),
                skip_nulls=False,
                return_dtype=pl.Utf8,
            ),
            (pl.col(column) * 100)
            .fill_null(0)
            .map_elements(
                lambda x: "{:.1f}%".format(x).replace(".", ","),
                skip_nulls=False,
                return_dtype=pl.Utf8,
            ),
        )
        st.write(f"Products with lowest prices for {shop1} (rank = 1)")
        st.dataframe(df_de_sorted1_ranked, hide_index=True, width='stretch')
        st.divider()
        df_de_sorted12_ranked = df_de_sorted12_ranked.with_columns(
            pl.col("article").map_elements(
                lambda x: "{:,}".format(x).replace(",", ""),
                skip_nulls=False,
                return_dtype=pl.Utf8,
            ),
            pl.col(column2).map_elements(
                lambda x: "{:,}".format(x).replace(".", ","),
                skip_nulls=False,
                return_dtype=pl.Utf8,
            ),
            (pl.col(column) * 100)
            .fill_null(0)
            .map_elements(
                lambda x: "{:.1f}%".format(x).replace(".", ","),
                skip_nulls=False,
                return_dtype=pl.Utf8,
            ),
        )
        st.write(f"Products with lowest prices for {shop1} (rank = 2)")
        st.dataframe(df_de_sorted12_ranked, hide_index=True, width='stretch')

    with coln3:
        df_de_sorted2_ranked = df_de_sorted2_ranked.with_columns(
            pl.col("article").map_elements(
                lambda x: "{:,}".format(x).replace(",", ""),
                skip_nulls=False,
                return_dtype=pl.Utf8,
            ),
            pl.col(column2).map_elements(
                lambda x: "{:,}".format(x).replace(".", ","),
                skip_nulls=False,
                return_dtype=pl.Utf8,
            ),
            (pl.col(column) * 100)
            .fill_null(0)
            .map_elements(
                lambda x: "{:.1f}%".format(x).replace(".", ","),
                skip_nulls=False,
                return_dtype=pl.Utf8,
            ),
        )
        st.write(f"Products with lowest prices for {shop2} (rank = 1)")
        st.dataframe(df_de_sorted2_ranked, hide_index=True, width='stretch')
        st.divider()
        df_de_sorted22_ranked = df_de_sorted22_ranked.with_columns(
            pl.col("article").map_elements(
                lambda x: "{:,}".format(x).replace(",", ""),
                skip_nulls=False,
                return_dtype=pl.Utf8,
            ),
            pl.col(column2).map_elements(
                lambda x: "{:,}".format(x).replace(".", ","),
                skip_nulls=False,
                return_dtype=pl.Utf8,
            ),
            (pl.col(column) * 100)
            .fill_null(0)
            .map_elements(
                lambda x: "{:.1f}%".format(x).replace(".", ","),
                skip_nulls=False,
                return_dtype=pl.Utf8,
            ),
        )
        st.write(f"Products with lowest prices for {shop2} (rank = 2)")
        st.dataframe(df_de_sorted22_ranked, hide_index=True, width='stretch')

    st.divider()

    fig = go.Figure()
    colors = ["#7676bb", "#9fb3ba"]  # Add more colors if needed
    for shop, color in zip(
        [shop1, shop2],
        colors,
    ):  # Loop over the shops
        df_shop = df_de_disc.filter(
            pl.col("shop") == shop
        )  # Filter the DataFrame for the current shop
        fig.add_trace(
            go.Histogram(x=df_shop[column], nbinsx=10, name=shop, marker_color=color)
        )

    fig.update_layout(  # Set title and labels
        title_text=f"Discounts distribution for {date1.strftime('%d.%m.%Y')} for {shop1} and {shop2}",
        xaxis_title=None,
        yaxis_title="Quantity of products",
        bargap=0.2,
        bargroupgap=0.1,
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(
            tickformat=".0%",  # Format x-axis as percentage
        ),
        legend=dict(
            yanchor="bottom",
            y=0.95,  # Position legend below the graph
            xanchor="right",
            x=1,
            orientation="h",  # Horizontal orientation
            font=dict(size=16, color="#343499"),  # Increase font size
        ),
    )
    st.plotly_chart(fig, width='stretch')
    st.divider()

    col11, col12, col13, col14, col15, col16, col17 = st.columns([2, 2, 2, 1, 2, 2, 2])
    with col11:
        custom_metric(
            f"Price increases since day before {date1.strftime('%d.%m.%Y')} for {shop1}",
            price_changes(shop1)[0],
        )
        custom_metric(
            f"Price decreases since day before {date1.strftime('%d.%m.%Y')} for {shop1}",
            price_changes(shop1)[1],
        )
    with col12:
        custom_metric(
            f"Price increases since week before {date1.strftime('%d.%m.%Y')} for {shop1}",
            price_changes(shop1)[2],
        )
        custom_metric(
            f"Price decreases since week before {date1.strftime('%d.%m.%Y')} for {shop1}",
            price_changes(shop1)[3],
        )
    with col13:
        custom_metric(
            f"Price increases since month before {date1.strftime('%d.%m.%Y')} for {shop1}",
            price_changes(shop1)[4],
        )
        custom_metric(
            f"Price decreases since month before {date1.strftime('%d.%m.%Y')} for {shop1}",
            price_changes(shop1)[5],
        )
    with col14:
        st.empty()
    with col15:
        custom_metric(
            f"Price increases since day before {date1.strftime('%d.%m.%Y')} for {shop2}",
            price_changes(shop2)[0],
        )
        custom_metric(
            f"Price decreases since day before {date1.strftime('%d.%m.%Y')} for {shop2}",
            price_changes(shop2)[1],
        )
    with col16:
        custom_metric(
            f"Price increases since week before {date1.strftime('%d.%m.%Y')} for {shop2}",
            price_changes(shop2)[2],
        )
        custom_metric(
            f"Price decreases since week before {date1.strftime('%d.%m.%Y')} for {shop2}",
            price_changes(shop2)[3],
        )
    with col17:
        custom_metric(
            f"Price increases since month before {date1.strftime('%d.%m.%Y')} for {shop2}",
            price_changes(shop2)[4],
        )
        custom_metric(
            f"Price decreases since month before {date1.strftime('%d.%m.%Y')} for {shop2}",
            price_changes(shop2)[5],
        )
