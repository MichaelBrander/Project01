import plotly.graph_objs as go
import pandas as pd
import pdfkit
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
import logging


pdf_path = "stock_chart.pdf"
c = canvas.Canvas(pdf_path, pagesize=letter)
width, height = letter

parquet_file_path = '/Users/michaelb/Project 01/stock_data.parquet'

try:
    df = pd.read_parquet(parquet_file_path)
except Exception as e:
    logging.error(f"Error occurred while loading Parquet file: {e}")


df['timestamp'] = pd.to_datetime(df['timestamp'])
df.set_index('timestamp', inplace=True)
hourly_volume = df['volume'].resample('H').sum()

fig = go.Figure()

fig.add_trace(go.Scatter(x=df.index, y=df['price'], name='Price'))

fig.add_trace(go.Bar(x=hourly_volume.index, y=hourly_volume, name='Volume', yaxis='y2'))

fig.update_layout(
    yaxis=dict(title='Price'),
    yaxis2=dict(title='Volume', side='right', overlaying='y', showgrid=False),
    barmode='overlay',
)

fig.write_image("stock_chart.png")

image_path = "stock_chart.png"
image = ImageReader(image_path)
c.drawImage(image, 50, height - 400, width=500, height=300) 

c.save()
