import os
import sys
import math
import pickle
import numpy as np
import pandas as pd
from pathlib import Path

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from db import get_engine

# Output model path
ARTIFACT_DIR = ROOT_DIR / "artifacts"
LSTM_MODEL_PATH = ARTIFACT_DIR / "lstm_forecaster.pt"
SCALERS_PATH = ARTIFACT_DIR / "lstm_scalers.pkl"

class LSTMForecaster(nn.Module):
    def __init__(self, input_size=4, hidden_size=64, num_layers=2, output_size=4, forecast_horizon=7):
        super(LSTMForecaster, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.forecast_horizon = forecast_horizon
        self.output_size = output_size
        
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, forecast_horizon * output_size)
        
    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        
        out, _ = self.lstm(x, (h0, c0))
        out = out[:, -1, :] # last time step
        out = self.fc(out)  # shape: (batch, forecast_horizon * output_size)
        return out

class TimeSeriesDataset(Dataset):
    def __init__(self, X, y):
        self.X = torch.tensor(X, dtype=torch.float32)
        self.y = torch.tensor(y, dtype=torch.float32)
        
    def __len__(self):
        return len(self.X)
        
    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]

def load_aggregation_data():
    print("[DATA] Querying aggregate daily metrics from MySQL database...")
    engine = get_engine()
    
    query = """
        SELECT
            v.influencer_id,
            DATE(s.snapshot_at) AS snap_date,
            SUM(s.views_num) AS total_views,
            SUM(s.likes_num) AS total_likes,
            SUM(s.comments_num) AS total_comments,
            SUM(s.shares_num) AS total_shares
        FROM video_metrics_snapshot s
        INNER JOIN videos_echotik v ON v.video_pk = s.video_pk
        GROUP BY v.influencer_id, DATE(s.snapshot_at)
        ORDER BY v.influencer_id, snap_date
    """
    
    try:
        df = pd.read_sql(query, engine)
        print(f"[DATA] Loaded {len(df)} daily records from database.")
        return df
    except Exception as e:
        print(f"[DATA ERROR] Failed to query database: {e}. Falling back to synthetic training.")
        return pd.DataFrame()

def preprocess_and_sequence(df, seq_len=14, forecast_horizon=7, min_seq_len=7):
    """
    Build (X, y) pairs from per-influencer daily time-series.
    Uses adaptive windowing: if an influencer has fewer than (seq_len + forecast_horizon)
    days, falls back to min_seq_len if total days >= (min_seq_len + forecast_horizon).
    All sequences are padded / interpolated to seq_len length so shapes are uniform.
    """
    if df.empty:
        return np.array([]), np.array([])

    X_list = []
    y_list = []

    for influencer_id, group in df.groupby("influencer_id"):
        group = group.sort_values("snap_date").reset_index(drop=True)
        n_days = len(group)

        # Determine effective window length for this influencer
        if n_days >= seq_len + forecast_horizon:
            eff_seq = seq_len
        elif n_days >= min_seq_len + forecast_horizon:
            eff_seq = n_days - forecast_horizon  # use all available days minus forecast horizon
        else:
            continue  # not enough data even with relaxed window

        metrics = group[["total_views", "total_likes", "total_comments", "total_shares"]].values.astype(float)
        log_metrics = np.log1p(metrics)

        for i in range(n_days - eff_seq - forecast_horizon + 1):
            x_window_raw = log_metrics[i : i + eff_seq]
            y_window     = log_metrics[i + eff_seq : i + eff_seq + forecast_horizon]

            # Interpolate / resample x_window to standard seq_len so all tensors match
            if eff_seq == seq_len:
                x_window = x_window_raw
            else:
                old_idx = np.arange(eff_seq)
                new_idx = np.linspace(0, eff_seq - 1, seq_len)
                x_window = np.zeros((seq_len, 4))
                for col in range(4):
                    x_window[:, col] = np.interp(new_idx, old_idx, x_window_raw[:, col])

            X_list.append(x_window)
            y_list.append(y_window.flatten())

    if not X_list:
        return np.array([]), np.array([])

    return np.array(X_list), np.array(y_list)

def generate_synthetic_data(num_accounts=150, num_days=60, seq_len=14, forecast_horizon=7):
    print(f"[DATA] Generating synthetic time-series data: {num_accounts} accounts over {num_days} days...")
    X_list = []
    y_list = []
    
    np.random.seed(42)
    
    for a in range(num_accounts):
        base_views = np.random.exponential(scale=50000) + 100
        growth_rate = np.random.uniform(1.005, 1.04) # 0.5% to 4% daily growth
        
        views_seq = []
        likes_seq = []
        comments_seq = []
        shares_seq = []
        
        curr_v = base_views
        
        for d in range(num_days):
            noise = np.random.normal(loc=0.0, scale=0.02)
            curr_v = curr_v * (growth_rate + noise)
            
            # engagement rates with noise
            curr_l = curr_v * np.random.uniform(0.04, 0.12)
            curr_c = curr_v * np.random.uniform(0.005, 0.025)
            curr_s = curr_v * np.random.uniform(0.005, 0.035)
            
            views_seq.append(max(0, curr_v))
            likes_seq.append(max(0, curr_l))
            comments_seq.append(max(0, curr_c))
            shares_seq.append(max(0, curr_s))
            
        metrics = np.column_stack([views_seq, likes_seq, comments_seq, shares_seq])
        log_metrics = np.log1p(metrics)
        
        for i in range(len(log_metrics) - seq_len - forecast_horizon + 1):
            x_window = log_metrics[i : i + seq_len]
            y_window = log_metrics[i + seq_len : i + seq_len + forecast_horizon]
            X_list.append(x_window)
            y_list.append(y_window.flatten())
            
    return np.array(X_list), np.array(y_list)

def train_model():
    seq_len = 14
    forecast_horizon = 7
    input_size = 4
    output_size = 4

    df = load_aggregation_data()
    X_real, y_real = preprocess_and_sequence(df, seq_len, forecast_horizon)

    print(f"[DATA] Real sequences found: {len(X_real)}")

    if len(X_real) >= 5:
        # Have real data — augment with a small synthetic set for generalization
        X_syn, y_syn = generate_synthetic_data(
            num_accounts=100, num_days=60, seq_len=seq_len, forecast_horizon=forecast_horizon
        )
        X = np.concatenate([X_real, X_syn], axis=0)
        y = np.concatenate([y_real, y_syn], axis=0)
        print(f"[DATA] Training on {len(X_real)} real + {len(X_syn)} synthetic sequences = {len(X)} total")
    else:
        print("[DATA WARNING] Insufficient real sequence data (<5). Training on synthetic only.")
        X, y = generate_synthetic_data(
            num_accounts=150, num_days=60, seq_len=seq_len, forecast_horizon=forecast_horizon
        )

    dataset = TimeSeriesDataset(X, y)
    dataloader = DataLoader(dataset, batch_size=32, shuffle=True)

    device = torch.device("cuda" if torch.cuda.is_available() else ("mps" if torch.backends.mps.is_available() else "cpu"))
    print(f"[DEVICE] Training on device: {device}")

    model = LSTMForecaster(
        input_size=input_size,
        hidden_size=64,
        num_layers=2,
        output_size=output_size,
        forecast_horizon=forecast_horizon
    ).to(device)

    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    scheduler = optim.lr_scheduler.StepLR(optimizer, step_size=20, gamma=0.5)

    epochs = 60
    print(f"[TRAIN] Starting training for {epochs} epochs...")
    model.train()

    for epoch in range(epochs):
        epoch_loss = 0.0
        for batch_X, batch_y in dataloader:
            batch_X = batch_X.to(device)
            batch_y = batch_y.to(device)

            optimizer.zero_grad()
            predictions = model(batch_X)
            loss = criterion(predictions, batch_y)
            loss.backward()
            optimizer.step()

            epoch_loss += loss.item() * batch_X.size(0)

        scheduler.step()
        avg_loss = epoch_loss / len(dataset)
        if (epoch + 1) % 10 == 0 or epoch == 0:
            lr_now = optimizer.param_groups[0]["lr"]
            print(f"Epoch [{epoch+1}/{epochs}] - Loss: {avg_loss:.6f} - LR: {lr_now:.6f}")

    print("[SAVE] Saving model and scaler configuration...")
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    torch.save(model.state_dict(), LSTM_MODEL_PATH)

    scalers_meta = {
        "seq_len": seq_len,
        "forecast_horizon": forecast_horizon,
        "input_size": input_size,
        "output_size": output_size,
        "scaling": "log1p",
    }
    with open(SCALERS_PATH, "wb") as f:
        pickle.dump(scalers_meta, f)

    print(f"[SUCCESS] LSTM model successfully saved to {LSTM_MODEL_PATH}")
    print(f"[SUCCESS] Scaler metadata saved to {SCALERS_PATH}")

if __name__ == "__main__":
    train_model()
