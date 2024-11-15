import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk

# visual/visualization.py
import matplotlib.pyplot as plt

# 1. Gráfico para películas alquiladas por mes
def mostrar_grafico_peliculas_por_mes(df):
    df_pandas = df.toPandas()
    df_pandas.plot(kind="bar", x="mes", y="total_peliculas", title="Películas Alquiladas por Mes")
    plt.xlabel("Mes")
    plt.ylabel("Total de Películas Alquiladas")
    plt.show()

# 2. Gráfico para categorías de películas más alquiladas
def mostrar_grafico_categorias_mas_alquiladas(df):
    df_pandas = df.toPandas()
    df_pandas.plot(kind="bar", x="categoria", y="total_alquiladas", title="Categorías de Películas Más Alquiladas")
    plt.xlabel("Categoría")
    plt.ylabel("Total de Alquileres")
    plt.show()

def plot_clientes_mas_activos(df):
    # Convertir el DataFrame de PySpark a pandas
    df_pandas = df.toPandas()

    plt.figure(figsize=(10, 6))
    sns.barplot(x='total_alquiladas', y='cliente', data=df_pandas, palette='viridis')
    plt.title('Top 10 Clientes que han Alquilado Más Películas')
    plt.xlabel('Total Alquiladas')
    plt.ylabel('Cliente')
    plt.show()

def plot_peliculas_mas_populares(df):
    df = df.toPandas()
    plt.figure(figsize=(12, 6))
    sns.barplot(x='total_alquiladas', y='pelicula', data=df, palette='Blues_d')
    plt.title('Top 10 Películas Más Alquiladas')
    plt.xlabel('Total Alquiladas')
    plt.ylabel('Película')
    plt.show()

def plot_ingresos_mensuales(df):
    df = df.toPandas()
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df, x='mes', y='ingresos_mensuales', hue='anio', marker='o')
    plt.title('Distribución Mensual de Ingresos por Alquileres')
    plt.xlabel('Mes')
    plt.ylabel('Ingresos Mensuales ($)')
    plt.xticks(range(1, 13))
    plt.grid(True)
    plt.show()


def plot_heatmap_alquileres_por_categoria(df):
    # Convertir a pandas
    df_pandas = df.toPandas()

    # Pivotar el DataFrame para el heatmap (categoría en filas, mes en columnas)
    df_pivot = df_pandas.pivot(index="categoria", columns="mes", values="total_alquiladas")

    # Crear el heatmap
    plt.figure(figsize=(12, 8))
    sns.heatmap(df_pivot, annot=True, cmap="YlGnBu", fmt="d", linewidths=.5)
    plt.title('Alquileres por Categoría y Mes')
    plt.xlabel('Mes')
    plt.ylabel('Categoría')
    plt.show()