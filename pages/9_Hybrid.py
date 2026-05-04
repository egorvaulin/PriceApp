import polars as pl
import streamlit as st
from middleware import authenticate_user
import toml
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import pyarrow.parquet as pq
import io
from datetime import timedelta
import pandas as pd

config = toml.load("./.streamlit/secrets.toml")
key = config["secrets"]["data_key"].encode("utf-8")


def decrypt_data(data, key):
    cipher = AES.new(key, AES.MODE_CBC, iv=data[:16])
    pt = unpad(cipher.decrypt(data[16:]), AES.block_size)
    return pt


# Page configuration
st.set_page_config(
    page_title="Hybrid Analysis", layout="wide", initial_sidebar_state="expanded"
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
    st.markdown("## Analysis - Product Pricing Metrics")
    st.divider()

    with st.expander("ℹ️ How to use this page", expanded=False):
        st.markdown(
            """
            This page has two independent sections.

            ---

            ### Section 1 – Heatmap: Product / Customer Price Deviation

            **Step 1 – Select products and shops**
            - Use **"Select products"** (max 15) and **"Select shops"** (max 10) multiselects to choose what to compare.

            **Step 2 – Choose a comparison range**
            - **Last 90 days** – average price over the last 90 days vs Q4 2025 baseline.
            - **Last 10 days** – average price over the last 10 days vs Q4 2025 baseline.
            - **Last day** – latest available price vs Q4 2025 baseline.

            **Reading the heatmap**
            - Each cell shows the **% deviation** of the selected period's average price from the Q4 2025 average.
            - Colour scale: red = below baseline (price decreased), orange = slight increase (0–3%), yellow/green = moderate increase (3–10%), dark green = strong increase (10%+).
            - Empty cell = no data for that product/shop combination in the selected period.

            ---

            ### Section 2 – Prices by Shop

            **Step 1 – Select a shop and price type**
            - Choose a shop from the **"Select a shop"** dropdown.
            - Choose **"price"** or **"price_delivery"** to include or exclude shipping costs.

            **Reading the results table**
            | Column | Meaning |
            |---|---|
            Q4 2025 Avg | Average price in Q4 2025 (baseline) |
            Last 90d Avg | Average price over the last 90 days |
            Diff % (90d) | % change from Q4 baseline over 90 days |
            Last 10d Avg | Average price over the last 10 days |
            Diff % (10d) | % change from Q4 baseline over 10 days |
            Last Day | Most recent available price |
            Diff % (Last Day) | % change from Q4 baseline for the last day |

            Diff % colours: **red** = below 3%, **orange** = 3–10%, **green** = above 10%.
            """
        )

    @st.cache_data
    def load_data(path):
        with open(path, "rb") as f:
            encrypted_data = f.read()
            buffer = io.BytesIO(decrypt_data(encrypted_data, key))
            df = pl.read_parquet(buffer)
        return df

    # Load data
    df = load_data("./data/Ien.parquet")
    hnp = load_data("./data/tlp.parquet")
    hnp = hnp.with_columns(
        pl.col("article").cast(pl.Int32),
        pl.col("year").cast(pl.Int32)
    )

    # Filter German data
    df_de = (
        df.filter(pl.col("country") == "de")
        .drop("country")
        .with_columns(year=pl.col("date").dt.year())
    )

    # Get all unique products and shops from df_de
    all_products = df_de.select(pl.col("article")).unique().join(
        hnp.select(pl.col("article", "product")).unique(),
        on="article",
        how="left"
    ).sort("product")["product"].to_list()
    all_products = [p for p in all_products if p is not None]
    
    all_shops = df_de.select(pl.col("shop")).unique().sort("shop")["shop"].to_list()
    all_shops = [s for s in all_shops if s is not None]
    
    # Heatmap section
    st.markdown("## Heatmap - Product / Customer Price Deviation")
    st.divider()
    
    # Product and shop selection for heatmap
    heatmap_col1, heatmap_col2, heatmap_col3 = st.columns([2, 2, 2], gap="medium")
    
    with heatmap_col1:
        selected_products = st.multiselect(
            "Select products (max 15)", 
            all_products,
            max_selections=15,
            key="products_select"
        )
    
    with heatmap_col2:
        selected_shops = st.multiselect(
            "Select shops (max 10)", 
            all_shops,
            max_selections=10,
            key="shops_select"
        )
    
    with heatmap_col3:
        heatmap_period = st.selectbox("Select comparison range", 
                                      ["Last 90 days", "Last 10 days", "Last day"])
    
    # Check if selections are made
    if not selected_products or not selected_shops:
        st.info("Please select at least one product and one shop to display the heatmap.")
    else:
        # Prepare heatmap data
        heatmap_results = []
        
        # Get selected articles mapping
        selected_articles_data = df_de.select(pl.col("article")).unique().join(
            hnp.select(pl.col("article", "product")).unique(),
            on="article",
            how="left"
        )
        
        for row in selected_articles_data.iter_rows(named=True):
            article = row["article"]
            product = row["product"]
            
            # Skip if product not selected
            if product not in selected_products:
                continue
            
            product_data = df_de.filter(pl.col("article") == article)
            
            if product_data.is_empty():
                continue
            
            # Q4 2025 baseline
            q4_data = product_data.filter(
                (pl.col("date") >= pl.datetime(2025, 10, 1)) & 
                (pl.col("date") <= pl.datetime(2025, 12, 31))
            )
            q4_avg = q4_data.select(pl.col("price").mean())[0, 0] if not q4_data.is_empty() else None
            
            if q4_avg is None or q4_avg == 0:
                continue
            
            # Determine date range for comparison
            max_date_prod = product_data.select(pl.col("date").max())[0, 0]
            
            if heatmap_period == "Last 90 days":
                comparison_start = max_date_prod - timedelta(days=90)
            elif heatmap_period == "Last 10 days":
                comparison_start = max_date_prod - timedelta(days=10)
            else:  # Last day
                comparison_start = max_date_prod
            
            comparison_data = product_data.filter(pl.col("date") >= comparison_start)
            
            row_data = {"Product": product}
            
            # Calculate deviation for each selected customer
            for customer in selected_shops:
                customer_data = comparison_data.filter(pl.col("shop") == customer)
                
                if customer_data.is_empty():
                    row_data[customer] = None
                else:
                    customer_avg = customer_data.select(pl.col("price").mean())[0, 0]
                    deviation = ((customer_avg - q4_avg) / q4_avg) * 100
                    row_data[customer] = deviation
            
            heatmap_results.append(row_data)
        
        # Convert to pandas for heatmap styling
        heatmap_df = pd.DataFrame(heatmap_results)
        
        # Fill NaN values with empty strings for display
        display_heatmap = heatmap_df.fillna("")
        
        # Reorder columns: keep Product first, move columns with most empty values to the right
        product_col = display_heatmap["Product"]
        data_cols = display_heatmap.drop("Product", axis=1)
        
        # Count non-empty values per column
        non_empty_counts = (data_cols != "").sum()
        sorted_cols = non_empty_counts.sort_values(ascending=False).index.tolist()
        
        # Reorder dataframe
        display_heatmap = display_heatmap[["Product"] + sorted_cols]
        
        # Function for gradient color heatmap
        def color_heatmap_gradient(val):
            if val == "":
                return "background-color: white; color: black;"
            
            # Convert to float for color calculation
            try:
                num_val = float(str(val).replace("%", ""))
            except:
                return "background-color: white; color: black;"
            
            # Gradient colors
            if num_val < 0:
                # Red gradient: -30% = dark red, 0% = bright red
                intensity = max(0, min(1, (num_val + 30) / 30))
                rgb = (255, int(107 * intensity), int(107 * intensity))
            elif num_val < 3.0:
                # Red to Orange: 0% = red, 3% = orange
                progress = num_val / 3.0
                r = 255
                g = int(107 + (165 - 107) * progress)
                b = int(107 + (0 - 107) * progress)
                rgb = (r, g, b)
            elif num_val < 10.0:
                # Orange to Green: 3% = orange, 10% = green
                progress = (num_val - 3.0) / 7.0
                r = int(255 - (255 - 81) * progress)
                g = int(165 + (95 - 165) * progress)
                b = int(0 + (102 - 0) * progress)
                rgb = (r, g, b)
            else:
                # Green gradient: 10% = bright green, 30% = dark green
                intensity = min(1, (num_val - 10) / 20)
                rgb = (81 - int(81 * intensity), 207 - int(102 * intensity), 102)
            
            return f"background-color: rgb({int(rgb[0])}, {int(rgb[1])}, {int(rgb[2])}); color: white;"
        
        # Function to format heatmap values
        def format_heatmap_value(val):
            if val == "":
                return ""
            if isinstance(val, float):
                return f"{val:.2f}%"
            return val
        
        # Apply styling
        styled_heatmap = display_heatmap.style.map(
            color_heatmap_gradient,
            subset=[col for col in display_heatmap.columns if col != "Product"]
        ).format({
            col: format_heatmap_value for col in display_heatmap.columns if col != "Product"
        }).set_properties(**{'width': '40px !important', 'min-width': '40px', 'max-width': '40px', 'text-align': 'center'}, subset=[col for col in display_heatmap.columns if col != "Product"]
        ).set_properties(**{'width': '120px !important', 'min-width': '120px', 'max-width': '120px', 'text-align': 'left'}, subset=['Product']
        ).set_table_styles([
            {'selector': 'th', 'props': [('text-align', 'center'), ('width', '40px !important'), ('min-width', '40px'), ('max-width', '40px'), ('overflow', 'hidden'), ('white-space', 'nowrap')]},
            {'selector': 'td', 'props': [('width', '40px !important'), ('min-width', '40px'), ('max-width', '40px'), ('overflow', 'hidden'), ('text-overflow', 'ellipsis'), ('white-space', 'nowrap')]},
            {'selector': 'th:first-child', 'props': [('width', '120px !important'), ('min-width', '120px'), ('max-width', '120px')]},
            {'selector': 'td:first-child', 'props': [('width', '120px !important'), ('min-width', '120px'), ('max-width', '120px')]},
        ])
        
        st.dataframe(styled_heatmap, width='stretch', hide_index=True)

    st.divider()
    st.markdown("## Prices by Shop")
    st.divider()
    
    # Shop selection
    col1, col2 = st.columns([2, 2], gap="medium")
    
    with col1:
        available_shops = df_de["shop"].unique().sort().to_list()
        default_shop = "Amazon" if "Amazon" in available_shops else available_shops[0]
        selected_shop = st.selectbox("Select a shop", available_shops, index=available_shops.index(default_shop))

    with col2:
        price_type = st.selectbox("Price column", ["price", "price_delivery"])

    st.divider()
    shop_data = df_de.filter(pl.col("shop") == selected_shop).join(
        hnp.select(["article", "year", "product"]),
        on=["article", "year"],
        how="left",
    )

    # Get the max date in the dataset
    max_date = shop_data.select(pl.col("date").max())[0, 0]

    # Get date ranges
    q4_2025_start = pl.datetime(2025, 10, 1)
    q4_2025_end = pl.datetime(2025, 12, 31)
    last_90_days_start = max_date - timedelta(days=90)
    last_10_days_start = max_date - timedelta(days=10)

    # Get all articles with product names
    selected_articles = (
        df_de.select(pl.col("article"))
        .unique()
        .join(
            hnp.select(pl.col("article", "product")).unique(),
            on="article",
            how="left"
        )
    )

    # Build results table
    results = []

    for row in selected_articles.iter_rows(named=True):
        article = row["article"]
        product = row["product"]
        product_data = shop_data.filter(pl.col("article") == article)

        if product_data.is_empty():
            continue

        # Q4 2025 average
        q4_data = product_data.filter(
            (pl.col("date") >= q4_2025_start) & (pl.col("date") <= q4_2025_end)
        )
        q4_avg = q4_data.select(pl.col(price_type).mean())[0, 0] if not q4_data.is_empty() else None

        # Last 90 days average
        last_90_data = product_data.filter(pl.col("date") >= last_90_days_start)
        last_90_avg = last_90_data.select(pl.col(price_type).mean())[0, 0] if not last_90_data.is_empty() else None

        # Last 10 days average
        last_10_data = product_data.filter(pl.col("date") >= last_10_days_start)
        last_10_avg = last_10_data.select(pl.col(price_type).mean())[0, 0] if not last_10_data.is_empty() else None

        # Last day price
        last_day_data = product_data.filter(pl.col("date") == max_date)
        last_day_price = last_day_data.select(pl.col(price_type).mean())[0, 0] if not last_day_data.is_empty() else None

        # Calculate percentage differences from Q4 2025
        pct_diff_90 = None
        pct_diff_10 = None
        pct_diff_last_day = None

        if q4_avg is not None and q4_avg > 0:
            if last_90_avg is not None:
                pct_diff_90 = ((last_90_avg - q4_avg) / q4_avg) * 100
            if last_10_avg is not None:
                pct_diff_10 = ((last_10_avg - q4_avg) / q4_avg) * 100
            if last_day_price is not None:
                pct_diff_last_day = ((last_day_price - q4_avg) / q4_avg) * 100

        results.append({
            "Article": article,
            "Product": product,
            "Q4 2025 Avg": round(q4_avg, 2) if q4_avg else None,
            "Last 90d Avg": round(last_90_avg, 2) if last_90_avg else None,
            "Diff % (90d)": pct_diff_90,
            "Last 10d Avg": round(last_10_avg, 2) if last_10_avg else None,
            "Diff % (10d)": pct_diff_10,
            "Last Day": round(last_day_price, 2) if last_day_price else None,
            "Diff % (Last Day)": pct_diff_last_day,
        })

    # Display results table
    st.divider()
    st.markdown(f"### {selected_shop} - Selected Metrics (Price type: {price_type})")
    
    # Convert to pandas for styling
    results_df = pd.DataFrame(results)
    
    # Define styling function for percentage columns
    def color_percentage(val):
        if pd.isna(val):
            return ""
        if val < 3.0:
            color = "red"
        elif val < 10.0:
            color = "orange"
        else:
            color = "green"
        return f"color: {color}; font-weight: bold;"
    
    # Apply styling
    styled_df = results_df.style.map(
        color_percentage,
        subset=["Diff % (90d)", "Diff % (10d)", "Diff % (Last Day)"]
    )
    
    # Format percentage columns with % sign
    def format_percentage(val):
        if pd.isna(val):
            return ""
        return f"{val:.2f}%"
    
    # Format numeric columns
    def format_number(val):
        if pd.isna(val):
            return ""
        return f"{val:.2f}"
    
    styled_df = styled_df.format({
        "Diff % (90d)": format_percentage,
        "Diff % (10d)": format_percentage,
        "Diff % (Last Day)": format_percentage,
        "Q4 2025 Avg": format_number,
        "Last 90d Avg": format_number,
        "Last 10d Avg": format_number,
        "Last Day": format_number,
    }, na_rep="")
    
    st.dataframe(styled_df, width='stretch', hide_index=True)
