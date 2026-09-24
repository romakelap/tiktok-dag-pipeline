#!/bin/bash
cd /home/ubuntu/dag-collection-tt
source venv/bin/activate
export $(grep -v '^#' .env | xargs)
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
