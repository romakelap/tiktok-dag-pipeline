import os
import sys
import logging
from pathlib import Path
import streamlit as st

# Setup Python Path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'plugins'))
sys.path.insert(0, str(project_root / 'config'))

from hooks.db_hook import EchotikDBHook
import run_pipeline_serverless

# Page Config
st.set_page_config(
    page_title="Echotik Pipeline Control Center",
    page_icon="📊",
    layout="wide",
)

# Initialize session state for persistence
if 'logs' not in st.session_state:
    st.session_state.logs = []
if 'pipeline_status' not in st.session_state:
    st.session_state.pipeline_status = 'idle'  # 'idle', 'running', 'success', 'failed'
if 'status_message' not in st.session_state:
    st.session_state.status_message = ''
if 'current_step' not in st.session_state:
    st.session_state.current_step = 1

# Custom Log Handler to stream logs into Streamlit UI in real-time
class StreamlitLogHandler(logging.Handler):
    def __init__(self, placeholder):
        super().__init__()
        self.placeholder = placeholder

    def emit(self, record):
        log_entry = self.format(record)
        st.session_state.logs.append(log_entry)
        
        # Parse log message to update progress step
        msg = record.getMessage()
        if "Starting Echotik Serverless Pipeline" in msg:
            st.session_state.current_step = 1
        elif "Starting data collection" in msg:
            st.session_state.current_step = 2
        elif "Exporting verified records to Excel" in msg:
            st.session_state.current_step = 3
        elif "Connecting to Aiven MySQL" in msg:
            st.session_state.current_step = 4
        elif "Loading Staging Tables" in msg:
            st.session_state.current_step = 5
        elif "Performing Production UPSERTs" in msg:
            st.session_state.current_step = 6
        elif "Refreshing BI Summary tables" in msg or "Writing Audit Log" in msg:
            st.session_state.current_step = 7
        elif "Pipeline Complete!" in msg:
            st.session_state.current_step = 8
            
        # Display logs in code box
        self.placeholder.code("\n".join(st.session_state.logs[-200:])) # Show last 200 logs

# CSS styling for premium look
st.markdown("""
<style>
    .reportview-container {
        background: #f0f2f6
    }
    .main-header {
        font-family: 'Outfit', 'Inter', sans-serif;
        color: #1E3A8A;
        font-weight: 700;
        margin-bottom: 20px;
    }
    .status-badge {
        padding: 5px 10px;
        border-radius: 5px;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("<h1 class='main-header'>📊 Echotik Ingestion Pipeline — Control Center</h1>", unsafe_allow_html=True)
st.write("Aplikasi web ini digunakan untuk memperbarui token Echotik dan memantau pipeline Tugas Akhir (TA) Anda secara langsung di cloud.")

# Initialize values from environment/secrets
default_db_conn = os.environ.get('DB_CONNECTION_STRING', '')
default_discord = os.environ.get('DISCORD_WEBHOOK_URL', '')

# Left and Right layout structure
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("🔑 Konfigurasi Credentials")
    
    # 1. Bearer Token Input (Always manual since it expires)
    st.markdown("**1. Token Echotik Baru**")
    token_input = st.text_input(
        "Masukkan Bearer Token dari browser (Authorization header):",
        placeholder="Format: 3083623|ZMmLGWMl2x...",
        type="password",
        help="Login ke echotik.live -> Buka F12 DevTools -> Cari network request -> Copy Authorization Header token"
    )
    
    # 2. Connection String Override
    st.markdown("**2. Database Connection (Aiven MySQL)**")
    db_conn_input = st.text_input(
        "DB Connection String URI:",
        value=default_db_conn,
        type="password",
        placeholder="mysql+pymysql://user:pass@host:port/dbname?ssl-mode=REQUIRED"
    )
    
    # 3. Discord Webhook Input
    st.markdown("**3. Discord Webhook URL (Opsional)**")
    discord_input = st.text_input(
        "Webhook URL:",
        value=default_discord,
        placeholder="https://discord.com/api/webhooks/..."
    )
    
    # Check DB Connection Button
    if st.button("🔌 Cek Koneksi Database", use_container_width=True):
        if not db_conn_input.strip():
            st.error("Connection string database kosong!")
        else:
            with st.spinner("Mencoba menghubungkan ke database..."):
                try:
                    db = EchotikDBHook(connection_string=db_conn_input)
                    if db.test_connection():
                        st.success("✅ Sukses terhubung ke database Aiven MySQL!")
                    else:
                        st.error("❌ Gagal terhubung ke database.")
                    db.close()
                except Exception as e:
                    st.error(f"❌ Error Koneksi: {e}")

with col2:
    st.subheader("🚀 Kontrol Pipeline")
    
    # Trigger button
    start_pipeline = st.button("🔥 Jalankan Pipeline Sekarang", type="primary", use_container_width=True)
    
    if start_pipeline:
        if not token_input.strip():
            st.error("Bearer Token Echotik tidak boleh kosong!")
        elif not db_conn_input.strip():
            st.error("Database Connection String tidak boleh kosong!")
        else:
            st.session_state.logs = []
            st.session_state.pipeline_status = 'running'
            st.session_state.status_message = ''
            st.session_state.current_step = 1
            
            # Set environment variables for this execution
            os.environ['ECHOTIK_BEARER_TOKEN'] = token_input.strip()
            os.environ['DB_CONNECTION_STRING'] = db_conn_input.strip()
            if discord_input.strip():
                os.environ['DISCORD_WEBHOOK_URL'] = discord_input.strip()
            else:
                os.environ.pop('DISCORD_WEBHOOK_URL', None)
                
    # Render logs and handle execution
    if st.session_state.pipeline_status != 'idle':
        st.markdown("### 🚦 Status Progress Pipeline")
        
        steps = [
            ("🔌 Inisialisasi & Setup Kredensial", 1),
            ("📥 Penarikan Data (3 API) dari Echotik", 2),
            ("📁 Export Hasil Penarikan ke File Excel", 3),
            ("🔑 Handshake Aiven MySQL (SSL)", 4),
            ("💾 Memuat Tabel Staging Database", 5),
            ("🔄 Upsert Data ke Tabel Produksi Utama", 6),
            ("📊 Refresh Ringkasan Chart BI & Audit Log", 7)
        ]
        
        curr = st.session_state.get('current_step', 1)
        
        # Render steps visually
        for name, step_num in steps:
            if curr > step_num:
                st.markdown(f"✅ **{name}**")
            elif curr == step_num:
                if st.session_state.pipeline_status == 'running':
                    st.markdown(f"🔄 **{name}** — *Sedang diproses...*")
                elif st.session_state.pipeline_status == 'success':
                    st.markdown(f"✅ **{name}**")
                else: # failed
                    st.markdown(f"❌ **{name}** — *Gagal*")
            else:
                st.markdown(f"⚪ `{name}`")
                
        st.markdown("---")
        st.markdown("### 📋 Logs Eksekusi (Real-Time)")
        log_placeholder = st.empty()
        
        # Pre-populate with existing logs
        if st.session_state.logs:
            log_placeholder.code("\n".join(st.session_state.logs[-200:]))
            
        if st.session_state.pipeline_status == 'running':
            # Setup handler
            handler = StreamlitLogHandler(log_placeholder)
            formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
            handler.setFormatter(formatter)
            
            root_logger = logging.getLogger()
            handler.setLevel(logging.INFO)
            root_logger.addHandler(handler)
            
            with st.spinner("Pipeline sedang berjalan..."):
                try:
                    # Run the serverless pipeline script main function
                    run_pipeline_serverless.main()
                    st.session_state.pipeline_status = 'success'
                    st.session_state.status_message = "🎯 Pipeline Selesai Sukses!"
                    st.session_state.current_step = 8
                except SystemExit as sys_exit:
                    if sys_exit.code == 0:
                        st.session_state.pipeline_status = 'success'
                        st.session_state.status_message = "🎯 Pipeline Selesai Sukses!"
                        st.session_state.current_step = 8
                    else:
                        st.session_state.pipeline_status = 'failed'
                        st.session_state.status_message = f"❌ Pipeline berhenti dengan kode error: {sys_exit.code}"
                except Exception as ex:
                    st.session_state.pipeline_status = 'failed'
                    st.session_state.status_message = f"❌ Pipeline Gagal: {ex}"
                finally:
                    # Cleanup handler to prevent duplicate logs on next run
                    root_logger.removeHandler(handler)
                    st.rerun() # Refresh to show final state cleanly
                    
        # Show final message
        if st.session_state.pipeline_status == 'success':
            st.success(st.session_state.status_message)
        elif st.session_state.pipeline_status == 'failed':
            st.error(st.session_state.status_message)

# Area Download File Excel
st.divider()
excel_dir = Path(project_root) / "raw-data" / "Excel"
if excel_dir.exists():
    excel_files = sorted(list(excel_dir.glob('echotik_data_*.xlsx')), key=os.path.getmtime, reverse=True)
    if excel_files:
        st.subheader("📥 Download File Excel Laporan (Hasil Run Terbaru)")
        
        # Show top 5 files in columns
        cols = st.columns(min(len(excel_files), 5))
        for i, file_path in enumerate(excel_files[:5]):
            with cols[i]:
                try:
                    with open(file_path, "rb") as f:
                        file_data = f.read()
                    st.download_button(
                        label=f"Download\n{file_path.name[-20:]}",  # Show short name
                        data=file_data,
                        file_name=file_path.name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"dl_{i}_{file_path.name}"
                    )
                    st.caption(f"File: `{file_path.name}`")
                except Exception as e:
                    st.error(f"Gagal memuat file: {file_path.name}")
    else:
        st.info("Belum ada file Excel yang di-generate. Silakan jalankan pipeline terlebih dahulu.")
else:
    st.info("Belum ada direktori Excel yang terbuat.")
