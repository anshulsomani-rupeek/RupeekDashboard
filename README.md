# Rupeek BI Dashboard

A high-performance, single-page Flask web application designed to act as a unified dashboard for executing and analyzing complex financial queries against the Rupeek Redshift Data Warehouse.

This tool replaces cumbersome DB IDE queries with a clean, dynamic, browser-based UI specifically tailored for analyzing Daybook logs, CLM/RCPL loan mappings, Customer Rupeek View (CRV) ledgers, and intricate Core Loan Requests.

## Features & Capabilities

The dashboard exposes several critical lookup modules via isolated side-by-side tabs:

1. **Loan Details Query:** Instantly fetches and visually joins data from `dw.account_svc_loan` using either a Lender account number or RCPL loan account number.
2. **Core Loan Request Query:** Paste a `GL Number` to run native lookups against both `dw.core_loanrequest_loan` (Secured) and `dw.core_loanrequest_uloan` (Unsecured), natively formatted with JSON pretty-printing.
3. **Daybook Analysis:** Paste a comma-separated list of `GL Numbers` to rapidly query and trace historical `lms_daybook1` narration logs.
4. **Mapping Lookup:** Cross-reference Rupeek IDs, LOS IDs, GL Numbers, and Reference Numbers securely via `temp.mapping`.
5. **CRV Data & Charges:** Full extraction dashboards for ledger histories (`dw.account_svc_customer_rupeek_view`), servicing charges (`dw.account_svc_charges`), and repayments (`dw.account_svc_repayment`).
6. **Excel Exports:** Native client-side data export to `.xlsx` built into every single query panel.

## Architecture

- **Backend:** Python + Flask backend utilizing `redshift_connector` for secure, low-latency AWS Redshift querying.
- **Frontend:** Pure HTML/JS/CSS single-page application (SPA). Features dynamic DOM generation, conditional coloring, JSON-parsers, and Excel exportation.
- **Security:** Credentials securely extracted from a local `.env` and injected upon boot. Prevents hardcoded credentials in source control.

## Installation & Setup

1. **Clone the project** into your secure local environment.
2. **Create a virtual environment** and install dependencies:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install flask redshift_connector python-dotenv
   ```
3. **Configure your Database Credentials:** 
   Create a `.env` file in the root directory mirroring the following structure:
   ```ini
   REDSHIFT_HOST=dw-redshift-prod.rupeek.com
   REDSHIFT_PORT=5439
   REDSHIFT_DB=datalake
   REDSHIFT_USER=your_username
   REDSHIFT_PASSWORD=your_password
   ```

## Running the Application

To comfortably start the server with your environment variables mapped, simply run the setup bash script:

```bash
chmod +x run.sh
./run.sh
```

Navigate to `http://localhost:5000` or `http://127.0.0.1:5000` in your preferred web browser to access the dashboard.

> **Note:** Access to the Redshift DB requires an active connection to the internal Rupeek VPN. If you receive DNS resolution errors, verify your VPN connection is active.