#!/bin/bash

# Activate the virtual environment
source venv/bin/activate

# Export required environment variables
export REDSHIFT_HOST=dw-redshift-prod.rupeek.com
export REDSHIFT_PORT=5439
export REDSHIFT_DB=datalake
export REDSHIFT_USER=anshul_somani_in
export REDSHIFT_PASSWORD='WZugN2xtqa65w4sQ'

# Run the application
if [ -f "app.py" ]; then
    python app.py
else
    echo "Error: app.py not found. Please ensure your application code is in this directory."
fi
