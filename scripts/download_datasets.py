"""
Script de Descarga Automatizada e Idempotente de Datasets (ADR-005)
DataMart S.A.S. - Pipeline ETL con Apache Airflow

Este script:
1. Verifica si data.csv y online_retail_II.csv existen en data/raw/.
2. Si ambos existen, finaliza inmediatamente sin descargas redundantes.
3. Si falta alguno, utiliza la librería oficial 'kagglehub' para descargarlo de Kaggle
   y ubicarlo de forma limpia en el directorio data/raw/.
"""

import os
import shutil
import glob
import sys

RAW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))

DATASET_CONFIGS = [
    {
        "name": "Transacciones de Ventas (data.csv)",
        "kaggle_handle": "carrie1/ecommerce-data",
        "target_filename": "data.csv",
        "expected_subnames": ["data.csv", "*ecommerce*.csv"]
    },
    {
        "name": "Historial Extendido (online_retail_II.csv)",
        "kaggle_handle": "thedevastator/online-retail-transaction-dataset",
        "alt_handle": "thedevastator/online-retail-transaction-data",
        "target_filename": "online_retail_II.csv",
        "expected_subnames": ["online_retail_II.csv", "*online_retail*.csv", "*.csv"]
    }
]


def ensure_directory():
    os.makedirs(RAW_DIR, exist_ok=True)


def file_is_valid(filepath: str, min_size_mb: float = 1.0) -> bool:
    if os.path.exists(filepath):
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        return size_mb >= min_size_mb
    return False


def download_with_kagglehub(config: dict):
    target_path = os.path.join(RAW_DIR, config["target_filename"])
    
    if file_is_valid(target_path):
        print(f"[OK] {config['name']} ya existe en: {target_path} ({os.path.getsize(target_path) / (1024*1024):.2f} MB)")
        return True

    print(f"[DESCARGANDO] {config['name']} desde Kaggle ({config['kaggle_handle']})...")
    
    try:
        import kagglehub
    except ImportError:
        print("[ERROR] 'kagglehub' no está instalado. Instalando dinámicamente...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "kagglehub"])
        import kagglehub

    download_dir = None
    try:
        download_dir = kagglehub.dataset_download(config["kaggle_handle"])
    except Exception as e:
        print(f"[WARN] Error con handle primario {config['kaggle_handle']}: {e}")
        if "alt_handle" in config:
            print(f"[REINTENTO] Intentando con handle alternativo {config['alt_handle']}...")
            try:
                download_dir = kagglehub.dataset_download(config["alt_handle"])
            except Exception as e2:
                print(f"[ERROR] Reintento fallido: {e2}")

    if not download_dir or not os.path.exists(download_dir):
        print(f"[ERROR] No se pudo obtener el directorio de descarga para {config['name']}")
        return False

    print(f"[INFO] Archivos descargados por kagglehub en: {download_dir}")

    # Buscar el archivo CSV correspondiente en la carpeta descargada
    found_file = None
    for pattern in config["expected_subnames"]:
        matches = glob.glob(os.path.join(download_dir, "**", pattern), recursive=True)
        if matches:
            found_file = matches[0]
            break

    if not found_file:
        # Buscar cualquier CSV disponible en el directorio
        all_csvs = glob.glob(os.path.join(download_dir, "**", "*.csv"), recursive=True)
        if all_csvs:
            found_file = all_csvs[0]

    if found_file and os.path.exists(found_file):
        print(f"[COPIANDO] {found_file} -> {target_path}")
        shutil.copy2(found_file, target_path)
        print(f"[ÉXITO] Archivo copiado exitosamente: {target_path} ({os.path.getsize(target_path) / (1024*1024):.2f} MB)")
        return True
    else:
        print(f"[ERROR] No se encontró ningún archivo CSV en el paquete descargado de {download_dir}")
        return False


def main():
    print("=" * 70)
    print("DATAMART S.A.S. - VERIFICADOR / DESCARGADOR DE DATASETS")
    print("=" * 70)
    ensure_directory()

    all_ready = True
    for config in DATASET_CONFIGS:
        success = download_with_kagglehub(config)
        if not success:
            all_ready = False

    if all_ready:
        print("=" * 70)
        print("[LISTO] Todos los datasets requeridos están disponibles en data/raw/")
        print("=" * 70)
        sys.exit(0)
    else:
        print("=" * 70)
        print("[AVISO] Algunos datasets no pudieron descargarse automáticamente.")
        print("Verifica tu conexión a internet o coloca manualmente los archivos en data/raw/.")
        print("=" * 70)
        sys.exit(1)


if __name__ == "__main__":
    main()
