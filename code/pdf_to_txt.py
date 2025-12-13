#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Conversión de PDF a Texto Plano
Taller: Caso Conteo de Palabras con Hadoop
"""

import subprocess
import os

# Rutas de archivos
PDF_FILE = "/home/esilvas/Documents/hadoop-python/files/Inteligencia_Rodriguez_ICE_2022.pdf"
OUTPUT_FILE = "/home/esilvas/Documents/hadoop-python/files/texto_plano.txt"

def pdf_to_text():
    """
    Convierte un archivo PDF a texto plano usando pdftotext
    """
    try:
        print(f"Convirtiendo PDF a texto plano...")
        print(f"Archivo origen: {PDF_FILE}")
        print(f"Archivo destino: {OUTPUT_FILE}")
        
        # Ejecutar pdftotext para extraer el texto
        subprocess.run([
            'pdftotext',
            PDF_FILE,
            OUTPUT_FILE
        ], check=True)
        
        # Verificar que se creó el archivo
        if os.path.exists(OUTPUT_FILE):
            # Obtener tamaño del archivo
            size = os.path.getsize(OUTPUT_FILE)
            print(f"\n✅ Conversión exitosa!")
            print(f"Archivo creado: {OUTPUT_FILE}")
            print(f"Tamaño: {size} bytes")
            
            # Contar líneas y palabras
            with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                words = sum(len(line.split()) for line in lines)
            
            print(f"Líneas: {len(lines)}")
            print(f"Palabras aproximadas: {words}")
        else:
            print("❌ Error: No se pudo crear el archivo de salida")
            
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al ejecutar pdftotext: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    pdf_to_text()
