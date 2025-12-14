#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de activación de servicios Hadoop (HDFS y YARN)
Taller: Caso Conteo de Palabras con Hadoop
"""

import subprocess
import sys

def ejecutar_comando(comando, descripcion):
    print(f"\n{'='*70}")
    print(f"📋 {descripcion}")
    print(f"{'='*70}")
    print(f"Comando: {' '.join(comando)}")
    print("-" * 70)
    try:
        result = subprocess.run(comando, capture_output=True, text=True, check=True)
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(f"Advertencias: {result.stderr}")
        print("✅ Comando ejecutado exitosamente")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al ejecutar el comando:")
        print(f"Código de salida: {e.returncode}")
        if e.stdout:
            print(f"Salida estándar: {e.stdout}")
        if e.stderr:
            print(f"Error: {e.stderr}")
        return False
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return False

def activar_hadoop():
    print("\n================ ACTIVACIÓN DE HADOOP ================\n")
    # 1. Iniciar el servicio SSH
    ejecutar_comando(['sudo', 'service', 'ssh', 'start'], "Iniciando el servicio SSH")
    # 2. Probar conexión SSH local
    ejecutar_comando(['ssh', '-o', 'StrictHostKeyChecking=no', 'localhost', 'exit'], "Probando conexión SSH local")
    # 3. Iniciar HDFS
    ejecutar_comando(['stop-dfs.sh'], "Deteniendo HDFS (por si está corriendo)")
    ejecutar_comando(['start-dfs.sh'], "Iniciando HDFS (NameNode, DataNode, SecondaryNameNode)")
    # 4. Iniciar YARN
    ejecutar_comando(['stop-yarn.sh'], "Deteniendo YARN (por si está corriendo)")
    ejecutar_comando(['start-yarn.sh'], "Iniciando YARN (ResourceManager, NodeManager)")
    # 5. Verificar servicios activos
    ejecutar_comando(['jps'], "Verificando procesos Java activos (Hadoop)")
    print("\n================ HADOOP ACTIVADO ================\n")

if __name__ == "__main__":
    activar_hadoop()
