# techretail_etl_completo.py - Proceso ETL Completo para TechRetail
import pandas as pd
import numpy as np
from datetime import datetime
import os

print("INICIANDO PROCESO ETL PARA TECHRETAIL")
print("=" * 50)

# =============================================================================
# 1. SIMULACION DE DATOS FUENTE
# =============================================================================
print("\nGENERANDO DATOS DE SIMULACION...")

# Ventas diarias (CSV)
ventas_data = {
    'fecha': ['2025-01-15', '2025-01-15', '2025-01-16', '2025-01-16', '2025-01-17', '2025-01-17', '2025-01-18', '2025-01-18'],
    'producto': ['Teclado Mecanico', 'Mouse Inalambrico', 'Monitor 24"', 'Teclado Mecanico', 'Mouse Inalambrico', 'Tablet', 'Monitor 24"', 'Auriculares'],
    'cantidad': [2, 3, 1, 1, 4, 2, 2, 3],
    'codigo_region': [1, 2, 1, 3, 2, 1, 4, 2],
    'precio_unitario': [80, 25, 300, 80, 25, 150, 300, 45]
}

# Catalogo de productos (CSV en lugar de Excel)
productos_data = {
    'producto': ['teclado mecanico', 'mouse inalambrico', 'monitor 24"', 'tablet', 'auriculares'],
    'categoria': ['Perifericos', 'Perifericos', 'Monitores', 'Dispositivos Moviles', 'Audio'],
    'precio_referencia': [80, 25, 300, 150, 45],
    'proveedor': ['TechCorp', 'ConnectPlus', 'ViewMaster', 'MobileTech', 'SoundMax']
}

# Regiones (CSV)
regiones_data = {
    'codigo_region': [1, 2, 3, 4],
    'nombre_region': ['Norte', 'Sur', 'Este', 'Oeste'],
    'zona': ['Nacional', 'Nacional', 'Nacional', 'Nacional']
}

# Crear DataFrames
ventas = pd.DataFrame(ventas_data)
productos = pd.DataFrame(productos_data)
regiones = pd.DataFrame(regiones_data)

# Guardar archivos fuente (SOLO CSV)
ventas.to_csv('ventas_diarias.csv', index=False)
productos.to_csv('catalogo_productos.csv', index=False)  # Cambiado de Excel a CSV
regiones.to_csv('regiones.csv', index=False)

print("ARCHIVOS FUENTE GENERADOS:")
print(f"   - ventas_diarias.csv ({len(ventas)} registros)")
print(f"   - catalogo_productos.csv ({len(productos)} productos)")  # Actualizado
print(f"   - regiones.csv ({len(regiones)} regiones)")

# =============================================================================
# 2. EXTRACT - EXTRACCION DE DATOS
# =============================================================================
print("\nEXTRACT - EXTRACCION DE DATOS...")

try:
    # Leer archivos fuente (SOLO CSV)
    ventas_df = pd.read_csv('ventas_diarias.csv')
    productos_df = pd.read_csv('catalogo_productos.csv')  # Cambiado de Excel a CSV
    regiones_df = pd.read_csv('regiones.csv')
    
    print("EXTRACCION COMPLETADA:")
    print(f"   - Ventas: {len(ventas_df)} registros")
    print(f"   - Productos: {len(productos_df)} productos") 
    print(f"   - Regiones: {len(regiones_df)} regiones")
    
except Exception as e:
    print(f"ERROR EN EXTRACCION: {e}")

# =============================================================================
# 3. TRANSFORM - TRANSFORMACION Y LIMPIEZA
# =============================================================================
print("\nTRANSFORM - TRANSFORMACION DE DATOS...")

# Crear copia para transformacion
ventas_transform = ventas_df.copy()

# 1. Normalizacion de nombres de productos
print("1. Normalizando nombres de productos...")
ventas_transform['producto'] = ventas_transform['producto'].str.strip().str.lower()

# 2. Unir con catalogo de productos
print("2. Uniendo con catalogo de productos...")
ventas_transform = ventas_transform.merge(
    productos_df, 
    on='producto', 
    how='left',
    suffixes=('_venta', '_catalogo')
)

# 3. Calcular total de venta
print("3. Calculando totales de venta...")
ventas_transform['total_venta'] = ventas_transform['cantidad'] * ventas_transform['precio_unitario']

# 4. Asignar region
print("4. Asignando informacion de region...")
ventas_transform = ventas_transform.merge(
    regiones_df,
    on='codigo_region',
    how='left'
)

# 5. Manejar valores nulos
print("5. Validando calidad de datos...")
valores_nulos = ventas_transform.isnull().sum()
if valores_nulos.sum() > 0:
    print(f"   VALORES NULOS ENCONTRADOS: {valores_nulos[valores_nulos > 0].to_dict()}")
    # Rellenar categorias nulas
    ventas_transform['categoria'] = ventas_transform['categoria'].fillna('No Categorizado')
    ventas_transform['proveedor'] = ventas_transform['proveedor'].fillna('Desconocido')

# 6. Crear clave unica para venta
ventas_transform['id_venta'] = range(1, len(ventas_transform) + 1)

print("TRANSFORMACION COMPLETADA")
print(f"   - Registros transformados: {len(ventas_transform)}")

# =============================================================================
# 4. LOAD - CARGA DE DATOS CONSOLIDADOS
# =============================================================================
print("\nLOAD - CARGA DE DATOS CONSOLIDADOS...")

# Seleccionar y ordenar columnas para tabla final
columnas_finales = [
    'id_venta',
    'fecha',
    'producto',
    'categoria',
    'proveedor',
    'cantidad',
    'precio_unitario',
    'total_venta',
    'nombre_region',
    'zona',
    'codigo_region'
]

# Crear tabla final de hechos de ventas
hechos_ventas = ventas_transform[columnas_finales]

# Guardar archivos consolidados (SOLO CSV)
hechos_ventas.to_csv('hechos_ventas_consolidadas.csv', index=False)

print("CARGA COMPLETADA:")
print(f"   - hechos_ventas_consolidadas.csv ({len(hechos_ventas)} registros)")

# =============================================================================
# 5. VALIDACION Y ANALISIS DE RESULTADOS
# =============================================================================
print("\nVALIDACION Y ANALISIS DE RESULTADOS")
print("=" * 50)

# Mostrar resumen de la tabla consolidada
print("RESUMEN TABLA CONSOLIDADA:")
print(f"Periodo: {hechos_ventas['fecha'].min()} to {hechos_ventas['fecha'].max()}")
print(f"Total de ventas registradas: {len(hechos_ventas)}")
print(f"Ingreso total: ${hechos_ventas['total_venta'].sum():,.2f}")
print(f"Cantidad total vendida: {hechos_ventas['cantidad'].sum()} unidades")

print("\nPREVIEW DATOS CONSOLIDADOS:")
print(hechos_ventas.head(10))

# =============================================================================
# 6. RESPUESTAS A PREGUNTAS DE NEGOCIO
# =============================================================================
print("\nRESPUESTAS A PREGUNTAS DE NEGOCIO")
print("=" * 50)

# 1. Cuales son las categorias mas vendidas?
print("\n1. CATEGORIAS MAS VENDIDAS (por cantidad):")
categorias_cantidad = hechos_ventas.groupby('categoria').agg({
    'cantidad': 'sum',
    'total_venta': 'sum',
    'id_venta': 'count'
}).rename(columns={'id_venta': 'transacciones'})
categorias_cantidad = categorias_cantidad.sort_values('cantidad', ascending=False)
print(categorias_cantidad)

# 2. Que region genera mas ingresos?
print("\n2. REGION CON MAS INGRESOS:")
region_ingresos = hechos_ventas.groupby('nombre_region').agg({
    'total_venta': 'sum',
    'cantidad': 'sum',
    'id_venta': 'count'
}).rename(columns={'id_venta': 'transacciones'})
region_ingresos = region_ingresos.sort_values('total_venta', ascending=False)
print(region_ingresos)

# 3. Como varian las ventas a lo largo del tiempo?
print("\n3. VARIACION DE VENTAS EN EL TIEMPO:")
ventas_por_fecha = hechos_ventas.groupby('fecha').agg({
    'total_venta': 'sum',
    'cantidad': 'sum',
    'id_venta': 'count'
}).rename(columns={'id_venta': 'transacciones'})
print(ventas_por_fecha)

# =============================================================================
# 7. GENERACION DE REPORTES ADICIONALES
# =============================================================================
print("\nREPORTES ADICIONALES")
print("=" * 50)

# Top productos por ingresos
print("TOP 5 PRODUCTOS POR INGRESOS:")
top_productos = hechos_ventas.groupby('producto').agg({
    'total_venta': 'sum',
    'cantidad': 'sum'
}).sort_values('total_venta', ascending=False).head(5)
print(top_productos)

# Eficiencia por region
print("\nEFICIENCIA POR REGION (Ingreso promedio por transaccion):")
eficiencia_region = hechos_ventas.groupby('nombre_region').agg({
    'total_venta': ['sum', 'mean'],
    'id_venta': 'count'
})
eficiencia_region.columns = ['ingreso_total', 'ingreso_promedio', 'transacciones']
eficiencia_region = eficiencia_region.sort_values('ingreso_promedio', ascending=False)
print(eficiencia_region)

# =============================================================================
# 8. RESUMEN FINAL DEL PROCESO ETL
# =============================================================================
print("\nRESUMEN FINAL DEL PROCESO ETL")
print("=" * 50)

print(f"Fecha de ejecucion: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total registros procesados: {len(hechos_ventas)}")
print(f"Ingreso total procesado: ${hechos_ventas['total_venta'].sum():,.2f}")
print(f"Productos unicos: {hechos_ventas['producto'].nunique()}")
print(f"Regiones con ventas: {hechos_ventas['nombre_region'].nunique()}")
print(f"Archivos generados: 1 (CSV)")

print("\nPROCESO ETL COMPLETADO EXITOSAMENTE!")
print("El archivo 'hechos_ventas_consolidadas.csv' esta listo para analisis.")