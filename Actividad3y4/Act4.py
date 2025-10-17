# Actividad4_SIN_EMOJIS.py
import pandas as pd
from datetime import datetime

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
    
    # Crear datos de ejemplo
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

print("\n1. DRILL-DOWN: MES -> SEMANA")
print("(Ver ventas por mes y desglosar por semana)")
drill_down = hechos_ventas.groupby(['mes', 'semana']).agg({
    'total_venta': 'sum',
    'cantidad': 'sum'
}).round(2)
print(drill_down)

print("\n2. SLICE: REGION NORTE")
print("(Filtrar solo la region Norte)")
slice_region = hechos_ventas[hechos_ventas['nombre_region'] == 'Norte'].groupby('categoria').agg({
    'total_venta': 'sum',
    'cantidad': 'sum'
}).round(2)
print(slice_region)

print("\n3. SLICE: CATEGORIA PERIFERICOS")
print("(Filtrar solo la categoria Perifericos)")
slice_categoria = hechos_ventas[hechos_ventas['categoria'] == 'Perifericos'].groupby('nombre_region').agg({
    'total_venta': 'sum',
    'cantidad': 'sum'
}).round(2)
print(slice_categoria)

print("\n4. TABLA DINAMICA OLAP")
print("(Vista multidimensional)")
tabla_dinamica = hechos_ventas.pivot_table(
    values='total_venta',
    index=['nombre_region', 'categoria'],
    columns='mes',
    aggfunc='sum',
    fill_value=0
).round(2)
print(tabla_dinamica)

# Guardar tabla dinámica
tabla_dinamica.to_csv('tabla_dinamica_olap.csv')
print("\nTabla dinamica guardada: tabla_dinamica_olap.csv")

print("\n5. RESPUESTAS A PREGUNTAS DE NEGOCIO")

# Producto con mayor facturación
producto_top = hechos_ventas.groupby('producto')['total_venta'].sum().idxmax()
monto_producto = hechos_ventas.groupby('producto')['total_venta'].sum().max()
print(f"Producto con mayor facturacion: {producto_top} (${monto_producto:,.2f})")

# Región con mayor facturación
region_top = hechos_ventas.groupby('nombre_region')['total_venta'].sum().idxmax()
monto_region = hechos_ventas.groupby('nombre_region')['total_venta'].sum().max()
print(f"Region con mayor facturacion: {region_top} (${monto_region:,.2f})")

print("\n6. CONCLUSIONES DEL ANALISTA")

total_ventas = hechos_ventas['total_venta'].sum()
transacciones = len(hechos_ventas)
ticket_promedio = total_ventas / transacciones

print(f"""
METRICAS PRINCIPALES:
- Ingreso Total: ${total_ventas:,.2f}
- Transacciones: {transacciones}
- Ticket Promedio: ${ticket_promedio:,.2f}

HALLAZGOS:
1. El producto {producto_top} es el mas rentable
2. La region {region_top} genera los mayores ingresos
3. El analisis por tiempo muestra variaciones semanales
4. Las tablas dinamicas permiten analisis multidimensional

RECOMENDACIONES:
- Enfocar estrategias en {region_top} y {producto_top}
- Analizar el desempeno por categoria y region
- Usar estas herramientas para toma de decisiones
""")

# Guardar conclusiones
with open('conclusiones_act4.txt', 'w') as f:
    f.write("CONCLUSIONES ACTIVIDAD 4\n")
    f.write("=" * 30 + "\n\n")
    f.write(f"Producto mas facturado: {producto_top}\n")
    f.write(f"Region mas rentable: {region_top}\n")
    f.write(f"Ingreso total: ${total_ventas:,.2f}\n")
    f.write("El analisis OLAP permite identificar oportunidades especificas por region y categoria.")

print("Conclusiones guardadas: conclusiones_act4.txt")
print("\nACTIVIDAD 4 COMPLETADA EXITOSAMENTE")