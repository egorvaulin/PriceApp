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
key = config["secrets"]["data_key"].encode('utf-8')

def decrypt_data(data, key):
    cipher = AES.new(key, AES.MODE_CBC, iv=data[:16])
    pt = unpad(cipher.decrypt(data[16:]), AES.block_size)
    return pt

# Page configuration
st.set_page_config(
    page_title="E-trader analysis",
    layout="wide",
    initial_sidebar_state="expanded")

# Change the font of the entire app
def set_font(font):
    st.markdown(f"""
                <style>
                body {{font-family: {font};}}
                </style>
                """, unsafe_allow_html=True)

set_font('Arial')

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
    st.markdown('## E-trader analysis')
    st.divider()

    @st.cache_data
    def load_data(path):
        with open(path, 'rb') as f:
            encrypted_data = f.read()
            buffer = io.BytesIO(decrypt_data(encrypted_data, key))
            df = pd.read_parquet(buffer, engine='pyarrow')
        return df

    def custom_metric(label, value):
        st.markdown(f"""
            <div style="border:1px solid #343499; border-left:8px solid #343499; border-radius:2px; padding:10px; margin:5px; text-align:center; height:180px; overflow:auto;">
                <h5>{label}</h5>
                <h2>{value}</h2>
            </div>
            """, unsafe_allow_html=True)

    df = load_data("./data/Ien.parquet")
    df_de = df[df['country'] == 'de']

    hnp = load_data("./data/hnp24.parquet")
    hnp.columns = hnp.columns.str.lower()

    df_de = df_de.merge(hnp[['article', 'hnp', 'subcategory', 'family', 'product']], on='article', how='left')
    df_de.drop(columns=['country'], inplace=True)
    df_de['date'] = pd.to_datetime(df_de['date']).dt.date
    df_de['disc1'] = 1 - df_de['price']/df_de['hnp']
    df_de['disc2'] = 1 - df_de['price_delivery']/df_de['hnp']
    cols =  ['month ago', 'week ago', 'day ago', 'last day']
    subcat = df_de['subcategory'].unique()

    st.markdown('### Analysis per e-trader')
    st.divider()

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        shop1 = st.selectbox('Select an e-trader', df_de['shop'].unique(), index=0)
    with col2:
        shop2 = st.selectbox('Select an e-trader', df_de['shop'].unique(), index=1)
    with col3:
        subcategory = st.selectbox('Select a subcategory', subcat, index=0)
        subcat_check = st.checkbox('Select this subcategory', value=False)
    with col4:
        disc = st.checkbox('Show for prices with delivery', value=False)
    with col5:
        date = st.selectbox('Select a period from', cols)

    shops = [shop1, shop2]

    df_de_shop = df_de[df_de['shop'].isin(shops)]
    last_day = df_de_shop['date'].max()
    previous_day = (df_de_shop['date'].max() - pd.DateOffset(days=1)).date()
    previous_week = (df_de_shop['date'].max() - pd.DateOffset(weeks=1)).date()
    previous_month = (df_de_shop['date'].max() - pd.DateOffset(months=1)).date()
    if date == 'month ago':
        df_de_shop1 = df_de_shop[df_de_shop['date'] >= previous_month]
        df_de = df_de[df_de['date'] >= previous_month]
    elif date == 'week ago':
        df_de_shop1 = df_de_shop[df_de_shop['date'] >= previous_week]
        df_de = df_de[df_de['date'] >= previous_week]
    elif date == 'day ago':
        df_de_shop1 = df_de_shop[df_de_shop['date'] >= previous_day]
        df_de = df_de[df_de['date'] >= previous_day]
    else:
        df_de_shop1 = df_de_shop[df_de_shop['date'] == last_day]
        df_de = df_de[df_de['date'] == last_day]

    def price_changes(shop):
        df_de_shopf = df_de_shop[df_de_shop['shop'] == shop]
        filtered_df = df_de_shopf[df_de_shopf['date'].isin([last_day, previous_day, previous_week, previous_month])].reset_index(drop=True)

        pivot_df = pd.pivot_table(filtered_df, values='price', index='product', columns='date')
        pivot_df = pivot_df.rename(dict(zip(pivot_df.columns, cols)), axis=1)
        pivot_df = pivot_df.iloc[:, ::-1].dropna(subset=['last day']).sort_values(by='last day', ascending=True)
        pivot_df.iloc[:, 1:] = pivot_df.iloc[:, 0:1].values - pivot_df.iloc[:, 1:]
        # sorted_df = pivot_df.sort_values(['week ago'], ascending=change).dropna(subset=['week ago']) # Sort should be changed depending from the selection and ascending or descending

        day_ago_positive = pivot_df['day ago'][pivot_df['day ago'] > 0].count()  # Count positive and negative values in 'day ago' column
        day_ago_negative = pivot_df['day ago'][pivot_df['day ago'] < 0].count()

        week_ago_positive = pivot_df['week ago'][pivot_df['week ago'] > 0].count()  # Count positive and negative values in 'week ago' column
        week_ago_negative = pivot_df['week ago'][pivot_df['week ago'] < 0].count()

        month_ago_positive = pivot_df['month ago'][pivot_df['month ago'] > 0].count()  # Count positive and negative values in 'month ago' column
        month_ago_negative = pivot_df['month ago'][pivot_df['month ago'] < 0].count()
        return [day_ago_positive, day_ago_negative, week_ago_positive, week_ago_negative, month_ago_positive, month_ago_negative, pivot_df]

    if subcat_check:
        df_de_show = df_de_shop1[df_de_shop1['subcategory'] == subcategory]
        df_de = df_de[df_de['subcategory'] == subcategory]
    else:
        df_de_show = df_de_shop1
        df_de = df_de

    column = 'disc1' if not disc else 'disc2'
    df_de_sorted = df_de.sort_values(['date', 'price']) # Sort the DataFrame by date and price
    grouped = df_de_sorted.groupby(['date', 'article']) # Group the DataFrame by date and article
    df_de_sorted['rank'] = grouped['price'].rank(method='min') # Calculate the rank of each price
    shop_rank_counts = df_de_sorted.groupby(['shop', 'rank']).size().reset_index(name='counts') # Count the number of products with each rank for each shop
    shop_rank_counts_1 = shop_rank_counts[shop_rank_counts['shop'] == shop1] # Filter the DataFrame for the first shop
    shop_rank_counts_2 = shop_rank_counts[shop_rank_counts['shop'] == shop2] # Filter the DataFrame for the second shop
    shop_rank_counts_1 = shop_rank_counts_1.reset_index(drop=True) # Reset the index
    shop_rank_counts_2 = shop_rank_counts_2.reset_index(drop=True) # Reset the index

    df_de_sorted1 = df_de_sorted[df_de_sorted['shop'] == shop1]
    df_de_sorted1_ranked = df_de_sorted1[df_de_sorted1['rank'] == 1]
    df_de_sorted1_ranked = df_de_sorted1_ranked[['article', 'product', 'price', 'disc1']].reset_index(drop=True)
    df_de_sorted2 = df_de_sorted[df_de_sorted['shop'] == shop2]
    df_de_sorted2_ranked = df_de_sorted2[df_de_sorted2['rank'] == 1]
    df_de_sorted2_ranked = df_de_sorted2_ranked[['article', 'product', 'price', 'disc1']].reset_index(drop=True)

    st.divider()
    coln1, coln2 = st.columns([1, 2])
    with coln1:
        shop_rank_counts_1['counts'] = shop_rank_counts_1['counts'].apply(lambda x: '{:,}'.format(x).replace(',', '.'))
        shop_rank_counts_1.index = shop_rank_counts_1.index + 1
        st.write(f'Rank counts for {shop1}')
        st.dataframe(shop_rank_counts_1.head(10))
    with coln2:
        df_de_sorted1_ranked['article'] = df_de_sorted1_ranked['article'].apply(lambda x: '{:,}'.format(x).replace(',', ''))
        df_de_sorted1_ranked['price'] = df_de_sorted1_ranked['price'].apply(lambda x: '{:,}'.format(x).replace('.', ','))
        df_de_sorted1_ranked['disc1'] = (df_de_sorted1_ranked['disc1'] * 100).apply(lambda x: '{:.1f}%'.format(x)).replace('.', ',')
        df_de_sorted1_ranked = df_de_sorted1_ranked.rename(columns={'disc1': 'discount'})
        df_de_sorted1_ranked = df_de_sorted1_ranked.sort_values(by=['discount', 'product'], ascending=[False, True], na_position='last')
        df_de_sorted1_ranked = df_de_sorted1_ranked.reset_index(drop=True)
        df_de_sorted1_ranked.index = df_de_sorted1_ranked.index + 1
        st.write(f'Products with lowest prices for {shop1} (rank = 1)')
        st.dataframe(df_de_sorted1_ranked)

    coln3, coln4 = st.columns([1, 2])
    with coln3:
        shop_rank_counts_2['counts'] = shop_rank_counts_2['counts'].apply(lambda x: '{:,}'.format(x).replace(',', '.'))
        shop_rank_counts_2.index = shop_rank_counts_2.index + 1
        st.write(f'Rank counts for {shop2}')
        st.dataframe(shop_rank_counts_2.head(10))
    with coln4:
        df_de_sorted2_ranked['article'] = df_de_sorted2_ranked['article'].apply(lambda x: '{:,}'.format(x).replace(',', ''))
        df_de_sorted2_ranked['price'] = df_de_sorted2_ranked['price'].apply(lambda x: '{:,}'.format(x).replace('.', ','))
        df_de_sorted2_ranked['disc1'] = (df_de_sorted2_ranked['disc1'] * 100).apply(lambda x: '{:.1f}%'.format(x)).replace('.', ',')
        df_de_sorted2_ranked = df_de_sorted2_ranked.rename(columns={'disc1': 'discount'})
        df_de_sorted2_ranked = df_de_sorted2_ranked.sort_values(by=['discount', 'product'], ascending=[False, True], na_position='last')
        df_de_sorted2_ranked = df_de_sorted2_ranked.reset_index(drop=True)
        df_de_sorted2_ranked.index = df_de_sorted2_ranked.index + 1
        st.write(f'Products with lowest prices for {shop2} (rank = 1)')
        st.dataframe(df_de_sorted2_ranked)
        
    fig = go.Figure()
    colors = ['#7676bb', '#9fb3ba']  # Add more colors if needed
    for shop, color in zip(shops, colors):  # Loop over the shops
        df_shop = df_de_show[df_de_show['shop'] == shop]  # Filter the DataFrame for the current shop
        fig.add_trace(go.Histogram(x=df_shop['disc1'], nbinsx=10, name=shop, marker_color=color))
        
    fig.update_layout(      # Set title and labels
        title_text= f'Discounts distribution for the period from {date} for {shops[0]} and {shops[1]}', 
        xaxis_title=None, 
        yaxis_title='Quantity of products', 
        bargap=0.2, 
        bargroupgap=0.1,
        plot_bgcolor='white',
        paper_bgcolor='white',
        xaxis=dict(
            tickformat=".0%",  # Format x-axis as percentage
        ),
        legend=dict(
            yanchor="bottom",
            y=0.95,  # Position legend below the graph
            xanchor="right",
            x=1,
            orientation="h",  # Horizontal orientation
            font=dict(
                size=16,  # Increase font size
                color='#343499'
            ),
        )
    )
    st.plotly_chart(fig, use_container_width=True)
    st.divider()

    col11, col12, col13, col14, col15, col16, col17 = st.columns([2, 2, 2, 1, 2, 2, 2])
    with col11:
        custom_metric(f'Price increases since day before for {shop1}', price_changes(shop1)[0])
        custom_metric(f'Price decreases since day before for {shop1}', price_changes(shop1)[1])
    with col12:
        custom_metric(f'Price increases since week before for {shop1}', price_changes(shop1)[2])
        custom_metric(f'Price decreases since week before for {shop1}', price_changes(shop1)[3])
    with col13:
        custom_metric(f'Price increases since month before for {shop1}', price_changes(shop1)[4])
        custom_metric(f'Price decreases since month before for {shop1}', price_changes(shop1)[5])
    with col14:
        st.empty()
    with col15:
        custom_metric(f'Price increases since day before for {shop2}', price_changes(shop2)[0])
        custom_metric(f'Price decreases since day before for {shop2}', price_changes(shop2)[1])
    with col16:
        custom_metric(f'Price increases since week before for {shop2}', price_changes(shop2)[2])
        custom_metric(f'Price decreases since week before for {shop2}', price_changes(shop2)[3])
    with col17:
        custom_metric(f'Price increases since month before for {shop2}', price_changes(shop2)[4])
        custom_metric(f'Price decreases since month before for {shop2}', price_changes(shop2)[5])
