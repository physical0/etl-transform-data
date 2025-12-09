import sys
import os
import importlib

# Add app to path to simulate Docker environment
sys.path.append(os.path.join(os.getcwd(), "app"))

def check_file(path):
    if os.path.exists(path):
        print(f"[OK] File exists: {path}")
        return True
    else:
        print(f"[FAIL] File missing: {path}")
        return False

def check_import(module_name):
    try:
        importlib.import_module(module_name)
        print(f"[OK] Import successful: {module_name}")
        return True
    except ImportError as e:
        print(f"[FAIL] Import failed for {module_name}: {e}")
        return False
    except Exception as e:
        print(f"[FAIL] Error importing {module_name}: {e}")
        return False

print("--- Verifying Project Structure ---")
files = [
    "app/Dockerfile",
    "app/requirements.txt",
    "app/etl/__init__.py",
    "app/etl/core/config.py",
    "app/etl/core/db.py",
    "app/etl/core/aws.py",
    "app/etl/models/crypto_model.py",
    "app/etl/schemas/crypto_schema.py",
    "app/etl/services/extractor.py",
    "app/etl/services/transformer.py",
    "app/etl/services/loader.py",
    "app/etl/pipeline/etl_job.py",
    "app/etl/run_etl.py",
    "airflow/dags/etl_dag.py"
]

all_files_ok = True
for f in files:
    if not check_file(f):
        all_files_ok = False

print("\n--- Verifying Imports ---")
# Note: We can't fully run them because of missing env vars/dependencies in this environment,
# but we can check if the modules are importable (syntax check + local imports).
# We mock boto3 and sqlalchemy if needed, but let's try raw first.

modules = [
    "etl.core.config",
    "etl.core.db",
    "etl.core.aws",
    "etl.models.crypto_model",
    "etl.schemas.crypto_schema",
    "etl.services.extractor",
    "etl.services.transformer",
    "etl.services.loader",
    "etl.pipeline.etl_job",
]

all_imports_ok = True
for m in modules:
    if not check_import(m):
        all_imports_ok = False

if all_files_ok and all_imports_ok:
    print("\n[SUCCESS] Validation Passed")
else:
    print("\n[FAILURE] Validation Failed")
