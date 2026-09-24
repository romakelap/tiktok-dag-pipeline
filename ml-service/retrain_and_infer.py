#!/usr/bin/env python3
"""
Quick retrain + inference runner.
Jalankan dari direktori ml-service:
    python3 retrain_and_infer.py
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent

def run(script, label):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    result = subprocess.run(
        [sys.executable, str(ROOT / script)],
        cwd=ROOT,
        capture_output=False,
    )
    if result.returncode != 0:
        print(f"[ERROR] {label} gagal (exit code {result.returncode})")
        sys.exit(result.returncode)
    print(f"[OK] {label} selesai")

if __name__ == "__main__":
    run("inference/train_lstm_forecaster.py", "STEP 1: Training LSTM model")
    run("inference/run_ml_inference.py",      "STEP 2: ML Inference → populate database")
    print("\n✅ Selesai! ml_engagement_forecasts sudah diperbarui.")
