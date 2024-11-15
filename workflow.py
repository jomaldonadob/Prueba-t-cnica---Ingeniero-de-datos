from queries.business_queries import (
    query_peliculas_alquiladas_por_mes,
    query_categorias_mas_alquiladas,
    query_clientes_mas_activos,
    query_peliculas_mas_populares,
    query_ingresos_mensuales,
    query_alquileres_por_categoria_mes,
)
from visual.visualization import (
    mostrar_grafico_peliculas_por_mes,
    mostrar_grafico_categorias_mas_alquiladas,
    plot_clientes_mas_activos,
    plot_peliculas_mas_populares,
    plot_ingresos_mensuales,
    plot_heatmap_alquileres_por_categoria,
)

# Define el flujo de trabajo como una lista de diccionarios
workflows = [
    {
        "query_function": query_peliculas_alquiladas_por_mes,
        "graph_function": mostrar_grafico_peliculas_por_mes,
        "description": "Películas alquiladas por mes",
        "params_required": ["spark", "rentals"],
    },
    {
        "query_function": query_categorias_mas_alquiladas,
        "graph_function": mostrar_grafico_categorias_mas_alquiladas,
        "description": "Categoria que más alquilan",
        "params_required": ["spark", "film", "rentals", "inventory"],
    },
    {
        "query_function": query_clientes_mas_activos,
        "graph_function": plot_clientes_mas_activos,
        "description": "Clientes mas activos",
        "params_required": ["spark", "rentals", "customers"],
    },
    {
        "query_function": query_peliculas_mas_populares,
        "graph_function": plot_peliculas_mas_populares,
        "description": "Peliculas mas populares",
        "params_required": ["spark", "film", "rentals", "inventory"],
    },
    {
        "query_function": query_ingresos_mensuales,
        "graph_function": plot_ingresos_mensuales,
        "description": "Ingresos Mensuales",
        "params_required": ["spark", "rentals", "film", "inventory"],
    },
    {
        "query_function": query_alquileres_por_categoria_mes,
        "graph_function": plot_heatmap_alquileres_por_categoria,
        "description": "Alquiles por categoria",
        "params_required": ["spark", "rentals", "film"],
    },

]
