import pandas as pd
import openpyxl
import re

def parse_routes_excel(file_path):
    """
    [Section 8.1] Parses the Excel route file and returns a list of stop records.
    """
    # Using openpyxl for reading XLSX
    wb = openpyxl.load_workbook(file_path)
    sheet = wb.active
    
    stops = []
    
    # Suponiendo que la primera fila es encabezado
    for row in sheet.iter_rows(min_row=2, values_only=True):
        # Column mapping (adjust based on actual file structure)
        # 0: numero_operador, 1: numero_ruta, 2: numero_parada, 3: Titulo, 
        # 4: Direccion, 5: Folio, 6: ID Referencia, 7: Ventana Horaria
        
        operador = str(row[0])
        ruta = str(row[1])
        parada = str(row[2])
        titulo = str(row[3])
        direccion = str(row[4])
        folio = str(row[5])
        referencia = str(row[6])
        ventana = str(row[7]) if len(row) > 7 else "Not Provided"
        
        # Extraer ID BOP (7 dígitos) del Titulo
        bop_match = re.search(r'\b(\d{7})\b', titulo)
        id_bop = bop_match.group(1) if bop_match else None
        
        # Cliente
        cliente = "Telcel" if titulo.startswith("Tel") else "Movistar" if titulo.startswith("Mo") else "Otro"
        
        record = {
            "numero_operador": operador,
            "numero_ruta": ruta,
            "numero_parada": parada,
            "id_bop": id_bop,
            "cliente": cliente,
            "direccion": direccion,
            "folio": folio,
            "id_referencia": referencia,
            "ventana_horaria": ventana
        }
        stops.append(record)
        
    return stops

# After extraction, each record is inserted into route_stops with final_status = PENDIENTE.
# Routes summary also created in 'routes' table.
