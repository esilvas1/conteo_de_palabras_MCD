#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Validación y Verificación de Hadoop
Taller: Caso Conteo de Palabras con Hadoop
"""

import subprocess
import sys

def ejecutar_comando(comando, descripcion):
    """
    Ejecuta un comando del sistema y muestra el resultado
    
    Args:
        comando: Lista con el comando y sus argumentos
        descripcion: Descripción del comando para mostrar
    """
    print(f"\n{'='*70}")
    print(f"📋 {descripcion}")
    print(f"{'='*70}")
    print(f"Comando: {' '.join(comando)}")
    print("-" * 70)
    
    try:
        result = subprocess.run(
            comando,
            capture_output=True,
            text=True,
            check=True
        )
        
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
    except FileNotFoundError:
        print(f"❌ Error: Comando no encontrado. ¿Está Hadoop instalado y en el PATH?")
        return False
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return False

def validar_hadoop():
    """
    Valida que Hadoop esté corriendo correctamente
    """
    print("\n" + "="*70)
    print("🔍 VALIDACIÓN DE HADOOP")
    print("="*70)
    
    # 1. Verificar procesos de Hadoop con jps
    success = ejecutar_comando(
        ['jps'],
        "Verificando servicios de Hadoop en ejecución"
    )
    
    if not success:
        print("\n⚠️ Advertencia: No se pudo verificar los servicios de Hadoop")
        print("Asegúrate de que Hadoop esté instalado y los servicios iniciados")
        respuesta = input("\n¿Deseas continuar de todas formas? (s/n): ")
        if respuesta.lower() != 's':
            sys.exit(1)
    
    # 2. Verificar versión de Hadoop
    ejecutar_comando(
        ['hadoop', 'version'],
        "Verificando versión de Hadoop"
    )
    
    # 3. Listar directorio raíz de HDFS
    ejecutar_comando(
        ['hdfs', 'dfs', '-ls', '/'],
        "Listando directorio raíz de HDFS"
    )
    
    # 4. Verificar el reporte de HDFS
    ejecutar_comando(
        ['hdfs', 'dfsadmin', '-report'],
        "Reporte del sistema de archivos HDFS"
    )
    
    print("\n" + "="*70)
    print("✅ Validación de Hadoop completada")
    print("="*70)
    print("\nSemáforo de validación de servicios Hadoop:")
    print("🟢 HDFS NameNode: http://localhost:9870")
    print("🟢 YARN ResourceManager: http://localhost:8088")
    print("🟡 IPC/CLI: hdfs dfs -ls / (usar solo por terminal)")
    print("🔴 No usar HTTP en puertos internos como 9000")
    print("\n🟢 = Servicio web activo | 🟡 = Solo terminal | 🔴 = Prohibido navegador\n")

if __name__ == "__main__":
    validar_hadoop()