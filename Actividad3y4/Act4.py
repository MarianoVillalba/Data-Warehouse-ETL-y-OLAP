# Actividad4_GRAFICO_FINAL.py
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

print("ACTIVIDAD 4 - ANALISIS OLAP")
print("=" * 40)

# Cargar datos de la Actividad 3
try:
    hechos_ventas = pd.read_csv('hechos_ventas_consolidadas.csv')
    print("DATOS CARGADOS CORRECTAMENTE")
    print(f"Registros cargados: {len(hechos_ventas)}")
except FileNotFoundError:
    print("ERROR: Primero ejecuta la Actividad 3")
    print("Creando datos de ejemplo para continuar...")
    
    datos_ejemplo = {
        'fecha': ['2025-01-15', '2025-01-15', '2025-01-16', '2025-01-16', '2025-01-17'],
        'producto': ['teclado mecanico', 'mouse inalambrico', 'monitor 24"', 'teclado mecanico', 'tablet'],
        'categoria': ['Perifericos', 'Perifericos', 'Monitores', 'Perifericos', 'Dispositivos Moviles'],
        'cantidad': [2, 3, 1, 1, 2],
        'precio_unitario': [80, 25, 300, 80, 150],
        'total_venta': [160, 75, 300, 80, 300],
        'nombre_region': ['Norte', 'Sur', 'Norte', 'Este', 'Norte']
    }
    hechos_ventas = pd.DataFrame(datos_ejemplo)
    print("Datos de ejemplo creados para la Actividad 4")

# Preparar datos temporales
hechos_ventas['fecha_dt'] = pd.to_datetime(hechos_ventas['fecha'])
hechos_ventas['mes'] = hechos_ventas['fecha_dt'].dt.month
hechos_ventas['semana'] = hechos_ventas['fecha_dt'].dt.isocalendar().week

# Análisis OLAP
print("\n1. DRILL-DOWN: MES -> SEMANA")
drill_down = hechos_ventas.groupby(['mes', 'semana']).agg({
    'total_venta': 'sum',
    'cantidad': 'sum'
}).round(2)
print(drill_down)

print("\n2. SLICE: REGION NORTE")
slice_region = hechos_ventas[hechos_ventas['nombre_region'] == 'Norte'].groupby('categoria').agg({
    'total_venta': 'sum',
    'cantidad': 'sum'
}).round(2)
print(slice_region)

print("\n3. SLICE: CATEGORIA PERIFERICOS")
slice_categoria = hechos_ventas[hechos_ventas['categoria'] == 'Perifericos'].groupby('nombre_region').agg({
    'total_venta': 'sum',
    'cantidad': 'sum'
}).round(2)
print(slice_categoria)

print("\n4. TABLA DINAMICA OLAP")
tabla_dinamica = hechos_ventas.pivot_table(
    values='total_venta',
    index=['nombre_region', 'categoria'],
    columns='mes',
    aggfunc='sum',
    fill_value=0
).round(2)
print(tabla_dinamica)
tabla_dinamica.to_csv('tabla_dinamica_olap.csv')
print("\nTabla dinamica guardada: tabla_dinamica_olap.csv")

# --- Cálculos principales (exactamente como en el txt) ---
producto_top = hechos_ventas.groupby('producto')['total_venta'].sum().idxmax()
monto_producto = hechos_ventas.groupby('producto')['total_venta'].sum().max()

region_top = hechos_ventas.groupby('nombre_region')['total_venta'].sum().idxmax()
monto_region = hechos_ventas.groupby('nombre_region')['total_venta'].sum().max()

total_ventas = hechos_ventas['total_venta'].sum()
transacciones = len(hechos_ventas)
ticket_promedio = total_ventas / transacciones

# --- Gráfico de resumen OLAP ---
plt.figure(figsize=(8, 6))
categorias = ['Producto más facturado', 'Región más rentable', 'Ingreso total', 'Ticket promedio']
valores = [monto_producto, monto_region, total_ventas, ticket_promedio]
colores = ['#4A90E2', '#F5A623', '#7ED321', '#9013FE']

plt.barh(categorias, valores, color=colores)
plt.title(
    f"Conclusiones OLAP\nProducto: {producto_top} | Región: {region_top}",
    fontsize=13, fontweight='bold', pad=15
)
plt.xlabel('Monto ($)')
plt.grid(axis='x', linestyle='--', alpha=0.6)

# Mostrar valores numéricos exactos sobre las barras
for i, valor in enumerate(valores):
    plt.text(valor + (valor * 0.02), i, f"${valor:,.2f}", va='center', fontsize=10)

plt.tight_layout()
plt.show()

print("\nACTIVIDAD 4 COMPLETADA EXITOSAMENTE")
