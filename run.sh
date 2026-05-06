#!/bin/bash
# run.sh — Launch the SmallCap Scanner dashboard

# Activate virtual environment
source venv/bin/activate 2>/dev/null || {
    echo "Virtual environment not found. Run: bash setup.sh first."
    exit 1
}

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║       SmallCap Scanner — Starting Up         ║"
echo "║   Open http://localhost:8501 in your browser ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

streamlit run app.py \
    --server.port 8501 \
    --server.headless false \
    --browser.gatherUsageStats false \
    --theme.base dark \
    --theme.primaryColor "#4ade80" \
    --theme.backgroundColor "#0d1117" \
    --theme.secondaryBackgroundColor "#161b22" \
    --theme.textColor "#e6edf3"
