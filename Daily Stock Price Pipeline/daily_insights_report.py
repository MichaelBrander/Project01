import pandas as pd
import numpy as np
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.platypus import Table, TableStyle
from datetime import datetime

# Load data
df = pd.read_parquet('C:\Projects\Project01\Container Airflow\outputs\stock_data.parquet')

# Ensure 'timestamp' is a datetime column
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Top price movements (minute to minute) for the trading day
df['timestamp'] = df['timestamp'].dt.tz_convert('Australia/Sydney')
df['time'] = df['timestamp'].dt.strftime('%H:%M')
df['changesPercentage_diff'] = df.groupby('symbol')['changesPercentage'].diff()
df['price_diff'] = df.groupby('symbol')['price'].diff()
top_movements = df.sort_values(by='price_diff', key=abs, ascending=False).head(10).round(2)
top_movements = top_movements.drop(columns=['timestamp'])


# Volatility Analysis
price_high = df.groupby('symbol')['price'].max()
price_low = df.groupby('symbol')['price'].min()
price_diff = (price_high - price_low)
opening_prices = df.groupby('symbol')['price'].first()
volatility = ((price_diff / opening_prices) * 100).round(2).sort_values(ascending=False)
volatility_df = volatility.reset_index(name='volatility_percentage')

# Stocks trading highest above their 50 day moving average


last_prices = df.groupby('symbol').tail(1)
top_50day_avg_stocks = last_prices[['symbol', 'price', 'priceAvg50']]
top_50day_avg_stocks['price_to_avg50_ratio'] = top_50day_avg_stocks['price'] / top_50day_avg_stocks['priceAvg50']
top_50day_avg_stocks = top_50day_avg_stocks.sort_values(by='price_to_avg50_ratio', ascending=False).round(2)
top_50day_avg_stocks = top_50day_avg_stocks.head(10)

# Generate PDF report
pdf_path = "stock_analysis_report.pdf"
c = canvas.Canvas(pdf_path, pagesize=letter)
width, height = letter

# Draw tables function
def draw_table(data, start_height, title, max_width=500):
    # Prepare data for Table
    data_list = [data.columns.values.tolist()] + data.values.tolist()
    table = Table(data_list)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('BOX', (0, 0), (-1, -1), 1, colors.black),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
    ]))

    # Adjust table size
    table.wrapOn(c, max_width, height)
    table_height = table._height
    table.drawOn(c, 72, start_height - table_height - 30)  # Adjust table start position

    # Title for the table
    c.setFont("Helvetica-Bold", 12)
    c.drawString(72, start_height - 15, title)


# Title
# Get today's date in the format Year-Month-Day
df['date'] = df['timestamp'].dt.date
report_date_str = df['date'].iloc[0].strftime("%Y-%m-%d")

# Title with dynamic date
c.setFont("Helvetica-Bold", 16)
c.drawString(72, height - 50, f"Stock Data Analysis Report - {report_date_str}")

# Drawing tables
draw_table(top_movements[['symbol', 'price', 'changesPercentage_diff', 'price_diff', 'time']], height - 90, "Top Minute-to-Minute Movements")
draw_table(volatility_df, height - 330, "Volatility Analysis - diff between day high, day low, over opening price")
draw_table(top_50day_avg_stocks, height - 560, "Top Stocks Above 50-Day Moving Average")

c.save()
print("PDF report generated: " + pdf_path)