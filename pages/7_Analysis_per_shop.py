import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from middleware import authenticate_user
import toml
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import pyarrow.parquet as pq
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
            df = pd.read_parquet(buffer, engine="pyarrow")
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
    df_de = df[df["country"] == "de"]

    hnp = load_data("./data/hnp24.parquet")
    hnp.columns = hnp.columns.str.lower()

    df_de = df_de.merge(
        hnp[["article", "hnp", "subcategory", "family", "product"]],
        on="article",
        how="left",
    )
    df_de.drop(columns=["country"], inplace=True)
    df_de["date"] = pd.to_datetime(df_de["date"]).dt.date
    df_de["disc1"] = 1 - df_de["price"] / df_de["hnp"]
    df_de["disc2"] = 1 - df_de["price_delivery"] / df_de["hnp"]
    subcat = df_de["subcategory"].unique()

    st.markdown("## Analysis per e-traders")
    st.divider()

    col1, col2, col3, col4 = st.columns(4, gap="medium")
    with col1:
        shop1 = st.selectbox("Select an e-trader", df_de["shop"].unique(), index=0)
    with col2:
        shop2 = st.selectbox("Select an e-trader", df_de["shop"].unique(), index=1)
    with col3:
        disc = st.checkbox("Show for prices with delivery", value=False)
    with col4:
        date = st.date_input(
            "Select a date",
            df_de["date"].max(),
            key="date_range1",
        )

    shops = [shop1, shop2]
    df_de_shop = df_de[df_de["shop"].isin(shops)]
    df_de = df_de[df_de["date"] == date]

    last_day = date
    previous_day = (date - pd.DateOffset(days=1)).date()
    previous_week = (date - pd.DateOffset(weeks=1)).date()
    previous_month = (date - pd.DateOffset(months=1)).date()
    dates = [previous_month, previous_week, previous_day, last_day]

    df_de_show = df_de_shop[df_de_shop["date"].isin(dates)]
    df_de_disc = df_de_shop[df_de_shop["date"] == date]
    column2 = "price_delivery" if disc else "price"

    def price_changes(shop):
        filtered_df = df_de_show[df_de_show["shop"] == shop]

        pivot_df = pd.pivot_table(
            filtered_df, values=column2, index="product", columns="date"
        )
        pivot_df = pivot_df.rename(dict(zip(pivot_df.columns, dates)), axis=1)
        pivot_df = (
            pivot_df.iloc[:, ::-1]
            .dropna(subset=[dates[-1]])
            .sort_values(by=dates[-1], ascending=True)
        )
        pivot_df.iloc[:, 1:] = pivot_df.iloc[:, 0:1].values - pivot_df.iloc[:, 1:]

        day_ago_positive = pivot_df[dates[-2]][
            pivot_df[dates[-2]] > 0
        ].count()  # Count positive and negative values in 'day ago' column
        day_ago_negative = pivot_df[dates[-2]][pivot_df[dates[-2]] < 0].count()

        week_ago_positive = pivot_df[dates[-3]][
            pivot_df[dates[-3]] > 0
        ].count()  # Count positive and negative values in 'week ago' column
        week_ago_negative = pivot_df[dates[-3]][pivot_df[dates[-3]] < 0].count()

        month_ago_positive = pivot_df[dates[-4]][
            pivot_df[dates[-4]] > 0
        ].count()  # Count positive and negative values in 'month ago' column
        month_ago_negative = pivot_df[dates[-4]][pivot_df[dates[-4]] < 0].count()
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
    df_de_sorted = df_de.sort_values(
        ["date", column2]
    )  # Sort the DataFrame by date and price
    grouped = df_de_sorted.groupby(
        ["date", "article"]
    )  # Group the DataFrame by date and article
    df_de_sorted["rank"] = grouped[column2].rank(
        method="min"
    )  # Calculate the rank of each price
    shop_rank_counts = (
        df_de_sorted.groupby(["shop", "rank"]).size().reset_index(name="counts")
    )  # Count the number of products with each rank for each shop
    shop_rank_counts_1 = shop_rank_counts[
        shop_rank_counts["shop"] == shop1
    ]  # Filter the DataFrame for the first shop
    shop_rank_counts_2 = shop_rank_counts[
        shop_rank_counts["shop"] == shop2
    ]  # Filter the DataFrame for the second shop
    shop_rank_counts_1 = shop_rank_counts_1.reset_index(drop=True)  # Reset the index
    shop_rank_counts_2 = shop_rank_counts_2.reset_index(drop=True)  # Reset the index

    df_de_sorted1 = df_de_sorted[df_de_sorted["shop"] == shop1]
    df_de_sorted1_ranked = df_de_sorted1[df_de_sorted1["rank"] == 1]
    df_de_sorted1_ranked = df_de_sorted1_ranked[
        ["article", "product", column2, column]
    ].reset_index(drop=True)
    df_de_sorted2 = df_de_sorted[df_de_sorted["shop"] == shop2]
    df_de_sorted2_ranked = df_de_sorted2[df_de_sorted2["rank"] == 1]
    df_de_sorted2_ranked = df_de_sorted2_ranked[
        ["article", "product", column2, column]
    ].reset_index(drop=True)

    coln1, coln2, coln3 = st.columns([2, 4, 4], gap="large")
    with coln1:
        shop_rank_counts_1["counts"] = shop_rank_counts_1["counts"].apply(
            lambda x: "{:,}".format(x).replace(",", ".")
        )
        shop_rank_counts_1.index = shop_rank_counts_1.index + 1
        st.write(f"Rank counts for {shop1}")
        st.dataframe(
            shop_rank_counts_1.head(3), hide_index=True, use_container_width=True
        )
        st.divider()
        shop_rank_counts_2["counts"] = shop_rank_counts_2["counts"].apply(
            lambda x: "{:,}".format(x).replace(",", ".")
        )
        shop_rank_counts_2.index = shop_rank_counts_2.index + 1
        st.write(f"Rank counts for {shop2}")
        st.dataframe(
            shop_rank_counts_2.head(3), hide_index=True, use_container_width=True
        )

    with coln2:
        df_de_sorted1_ranked["article"] = df_de_sorted1_ranked["article"].apply(
            lambda x: "{:,}".format(x).replace(",", "")
        )
        df_de_sorted1_ranked[column2] = df_de_sorted1_ranked[column2].apply(
            lambda x: "{:,}".format(x).replace(".", ",")
        )
        df_de_sorted1_ranked[column] = (
            (df_de_sorted1_ranked[column] * 100)
            .apply(lambda x: "{:.1f}%".format(x))
            .replace(".", ",")
        )
        df_de_sorted1_ranked = df_de_sorted1_ranked.rename(columns={column: "discount"})
        df_de_sorted1_ranked = df_de_sorted1_ranked.sort_values(
            by=["discount", "product"], ascending=[False, True], na_position="last"
        )
        df_de_sorted1_ranked = df_de_sorted1_ranked.reset_index(drop=True)
        df_de_sorted1_ranked.index = df_de_sorted1_ranked.index + 1
        st.write(f"Products with lowest prices for {shop1} (rank = 1)")
        st.dataframe(df_de_sorted1_ranked, hide_index=True, use_container_width=True)

    with coln3:
        df_de_sorted2_ranked["article"] = df_de_sorted2_ranked["article"].apply(
            lambda x: "{:,}".format(x).replace(",", "")
        )
        df_de_sorted2_ranked[column2] = df_de_sorted2_ranked[column2].apply(
            lambda x: "{:,}".format(x).replace(".", ",")
        )
        df_de_sorted2_ranked[column] = (
            (df_de_sorted2_ranked[column] * 100)
            .apply(lambda x: "{:.1f}%".format(x))
            .replace(".", ",")
        )
        df_de_sorted2_ranked = df_de_sorted2_ranked.rename(columns={column: "discount"})
        df_de_sorted2_ranked = df_de_sorted2_ranked.sort_values(
            by=["discount", "product"], ascending=[False, True], na_position="last"
        )
        df_de_sorted2_ranked = df_de_sorted2_ranked.reset_index(drop=True)
        df_de_sorted2_ranked.index = df_de_sorted2_ranked.index + 1
        st.write(f"Products with lowest prices for {shop2} (rank = 1)")
        st.dataframe(df_de_sorted2_ranked, hide_index=True, use_container_width=True)

    st.divider()

    fig = go.Figure()
    colors = ["#7676bb", "#9fb3ba"]  # Add more colors if needed
    for shop, color in zip(shops, colors):  # Loop over the shops
        df_shop = df_de_disc[
            df_de_disc["shop"] == shop
        ]  # Filter the DataFrame for the current shop
        fig.add_trace(
            go.Histogram(x=df_shop[column], nbinsx=10, name=shop, marker_color=color)
        )

    fig.update_layout(  # Set title and labels
        title_text=f"Discounts distribution for {date.strftime('%d.%m.%Y')} for {shops[0]} and {shops[1]}",
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
    st.plotly_chart(fig, use_container_width=True)
    st.divider()

    col11, col12, col13, col14, col15, col16, col17 = st.columns([2, 2, 2, 1, 2, 2, 2])
    with col11:
        custom_metric(
            f"Price increases since day before {date.strftime('%d.%m.%Y')} for {shop1}",
            price_changes(shop1)[0],
        )
        custom_metric(
            f"Price decreases since day before {date.strftime('%d.%m.%Y')} for {shop1}",
            price_changes(shop1)[1],
        )
    with col12:
        custom_metric(
            f"Price increases since week before {date.strftime('%d.%m.%Y')} for {shop1}",
            price_changes(shop1)[2],
        )
        custom_metric(
            f"Price decreases since week before {date.strftime('%d.%m.%Y')} for {shop1}",
            price_changes(shop1)[3],
        )
    with col13:
        custom_metric(
            f"Price increases since month before {date.strftime('%d.%m.%Y')} for {shop1}",
            price_changes(shop1)[4],
        )
        custom_metric(
            f"Price decreases since month before {date.strftime('%d.%m.%Y')} for {shop1}",
            price_changes(shop1)[5],
        )
    with col14:
        st.empty()
    with col15:
        custom_metric(
            f"Price increases since day before {date.strftime('%d.%m.%Y')} for {shop2}",
            price_changes(shop2)[0],
        )
        custom_metric(
            f"Price decreases since day before {date.strftime('%d.%m.%Y')} for {shop2}",
            price_changes(shop2)[1],
        )
    with col16:
        custom_metric(
            f"Price increases since week before {date.strftime('%d.%m.%Y')} for {shop2}",
            price_changes(shop2)[2],
        )
        custom_metric(
            f"Price decreases since week before {date.strftime('%d.%m.%Y')} for {shop2}",
            price_changes(shop2)[3],
        )
    with col17:
        custom_metric(
            f"Price increases since month before {date.strftime('%d.%m.%Y')} for {shop2}",
            price_changes(shop2)[4],
        )
        custom_metric(
            f"Price decreases since month before {date.strftime('%d.%m.%Y')} for {shop2}",
            price_changes(shop2)[5],
        )
