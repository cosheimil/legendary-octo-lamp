import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from datetime import datetime, timedelta

st.set_page_config(
    page_title="Fashion E-commerce Analytics", page_icon="👗", layout="wide"
)

engine = create_engine("postgresql://admin:admin@postgres/sales_db")

st.title("👗 Fashion E-commerce Analytics Dashboard")
st.markdown("### Анализ продаж Amazon и международных каналов")

st.sidebar.header("Фильтры")

try:
    col1, col2, col3, col4 = st.columns(4)

    amazon_data = pd.read_sql(
        """
        SELECT 
            SUM(total_revenue) as total_revenue,
            SUM(total_quantity) as total_quantity,
            SUM(order_count) as total_orders,
            COUNT(DISTINCT category) as categories_count
        FROM amazon_daily_sales
    """,
        engine,
    )

    with col1:
        st.metric(
            "Общая выручка (Amazon)", f"₹{amazon_data['total_revenue'].iloc[0]:,.0f}"
        )

    with col2:
        st.metric("Продано товаров", f"{int(amazon_data['total_quantity'].iloc[0]):,}")

    with col3:
        st.metric("Заказов", f"{int(amazon_data['total_orders'].iloc[0]):,}")

    with col4:
        st.metric("Категорий", f"{int(amazon_data['categories_count'].iloc[0])}")

    st.header("📊 Amazon Sales Analysis")

    daily_amazon = pd.read_sql(
        """
        SELECT 
            date,
            category,
            total_revenue,
            total_quantity,
            order_count
        FROM amazon_daily_sales
        ORDER BY date
    """,
        engine,
    )

    categories = daily_amazon["category"].unique().tolist()
    selected_categories = st.sidebar.multiselect(
        "Выберите категории",
        categories,
        default=categories[:5] if len(categories) > 5 else categories,
    )

    filtered_data = daily_amazon[daily_amazon["category"].isin(selected_categories)]

    fig_timeline = px.line(
        filtered_data,
        x="date",
        y="total_revenue",
        color="category",
        title="Динамика продаж по категориям",
        labels={"total_revenue": "Выручка (₹)", "date": "Дата"},
    )
    st.plotly_chart(fig_timeline, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        top_categories = pd.read_sql(
            """
            SELECT 
                category,
                SUM(total_revenue) as revenue
            FROM amazon_daily_sales
            GROUP BY category
            ORDER BY revenue DESC
            LIMIT 10
        """,
            engine,
        )

        fig_categories = px.bar(
            top_categories,
            x="revenue",
            y="category",
            orientation="h",
            title="Топ-10 категорий по выручке",
            labels={"revenue": "Выручка (₹)", "category": "Категория"},
        )
        st.plotly_chart(fig_categories, use_container_width=True)

    with col2:
        size_dist = pd.read_sql(
            """
            SELECT 
                size,
                COUNT(*) as count,
                SUM(amount) as revenue
            FROM amazon_sales_detail
            GROUP BY size
            ORDER BY count DESC
            LIMIT 10
        """,
            engine,
        )

        fig_sizes = px.pie(
            size_dist,
            values="count",
            names="size",
            title="Распределение заказов по размерам",
        )
        st.plotly_chart(fig_sizes, use_container_width=True)

    st.header("🌍 International Sales Analysis")

    intl_data = pd.read_sql(
        """
        SELECT 
            date,
            customer,
            SUM(quantity) as total_qty,
            SUM(revenue) as total_revenue
        FROM international_sales
        GROUP BY date, customer
        ORDER BY date DESC
    """,
        engine,
    )

    if not intl_data.empty:
        top_customers = pd.read_sql(
            """
            SELECT 
                customer,
                SUM(revenue) as revenue,
                SUM(quantity) as quantity
            FROM international_sales
            GROUP BY customer
            ORDER BY revenue DESC
            LIMIT 10
        """,
            engine,
        )

        fig_customers = px.bar(
            top_customers,
            x="customer",
            y="revenue",
            title="Топ-10 международных клиентов",
            labels={"revenue": "Выручка", "customer": "Клиент"},
        )
        st.plotly_chart(fig_customers, use_container_width=True)

        st.subheader("Детали международных продаж")
        st.dataframe(intl_data.head(20), use_container_width=True)
    else:
        st.info("Нет данных о международных продажах")

    st.header("📋 Детальная информация")

    tab1, tab2 = st.tabs(["Amazon Details", "SKU Analysis"])

    with tab1:
        detail_data = pd.read_sql(
            """
            SELECT 
                date,
                category,
                size,
                COUNT(*) as orders,
                SUM(quantity) as qty,
                SUM(amount) as revenue
            FROM amazon_sales_detail
            GROUP BY date, category, size
            ORDER BY date DESC
            LIMIT 100
        """,
            engine,
        )
        st.dataframe(detail_data, use_container_width=True)

    with tab2:
        sku_analysis = pd.read_sql(
            """
            SELECT 
                sku,
                category,
                COUNT(*) as order_count,
                SUM(quantity) as total_quantity,
                SUM(amount) as total_revenue,
                AVG(amount) as avg_order_value
            FROM amazon_sales_detail
            GROUP BY sku, category
            ORDER BY total_revenue DESC
            LIMIT 50
        """,
            engine,
        )

        st.dataframe(
            sku_analysis.style.format(
                {"total_revenue": "₹{:.2f}", "avg_order_value": "₹{:.2f}"}
            ),
            use_container_width=True,
        )

except Exception as e:
    st.error(f"Ошибка при загрузке данных: {e}")
    st.info("Убедитесь, что ETL pipeline выполнен и данные загружены в базу данных.")
    st.code("""
# Для запуска ETL выполните:
docker-compose exec prefect-server python flows/etl_flow.py
    """)

st.markdown("---")
st.markdown("🔄 Данные обновляются автоматически через Prefect ETL Pipeline")
