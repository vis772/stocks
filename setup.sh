#!/bin/bash
# setup.sh — First-time setup for the SmallCap Scanner
# Run this once: bash setup.sh
# Then run the app: bash run.sh

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║        SmallCap Scanner Setup            ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# Check Python version
python_version=$(python3 --version 2>&1)
echo "Python version: $python_version"

# Create virtual environment
echo ""
echo "Creating virtual environment..."
python3 -m venv venv

# Activate it
source venv/bin/activate

# Install dependencies
echo ""
echo "Installing dependencies..."
pip install --upgrade pip -q
pip install -r requirements.txt

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo ""
    echo "Creating .env file..."
    cat > .env << 'EOF'
# SmallCap Scanner Environment Variables
# ─────────────────────────────────────────────────────────────────────────────
# Optional: Add your Anthropic API key for AI-powered SEC filing summaries.
# Without this, the scanner runs fine but skips Claude-powered analysis.
# Get a key at: https://console.anthropic.com
ANTHROPIC_API_KEY=

# Optional: News API key (newsapi.org - free tier available)
# Without this, the scanner uses Yahoo Finance RSS feeds (free, works fine)
NEWS_API_KEY=
EOF
    echo ".env file created. Add your API keys there (optional but recommended)."
fi

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║         Setup complete!                  ║"
echo "║                                          ║"
echo "║  To run the scanner:                     ║"
echo "║    bash run.sh                           ║"
echo "║                                          ║"
echo "║  Or manually:                            ║"
echo "║    source venv/bin/activate              ║"
echo "║    streamlit run app.py                  ║"
echo "╚══════════════════════════════════════════╝"
echo ""
