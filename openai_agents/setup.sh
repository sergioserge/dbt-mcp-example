#!/bin/bash

echo "🚀 Setting up dbt AI Agent..."

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Activate conda environment
source /opt/homebrew/Caskroom/miniconda/base/etc/profile.d/conda.sh
conda activate dbt_env

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r "$SCRIPT_DIR/requirements.txt"

# Check if .env file exists
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    echo "📝 Creating .env file from template..."
    cp "$SCRIPT_DIR/env_template.txt" "$SCRIPT_DIR/.env"
    echo "⚠️  Please edit $SCRIPT_DIR/.env and add your OpenAI API key"
    echo "   Get your API key from: https://platform.openai.com/api-keys"
fi

# Check if OpenAI API key is set
if ! grep -q "OPENAI_API_KEY=sk-" "$SCRIPT_DIR/.env"; then
    echo "❌ Please set your OPENAI_API_KEY in $SCRIPT_DIR/.env"
    echo "   Edit the file and replace 'your_openai_api_key_here' with your actual API key"
    exit 1
fi

echo "✅ Setup complete!"
echo ""
echo "🎯 To run the dbt AI Agent:"
echo "   cd $SCRIPT_DIR"
echo "   python dbt_ai_agent.py"
echo ""
echo "💡 Example questions to try:"
echo "   - 'What models do I have in my dbt project?'"
echo "   - 'Show me the structure of my staging models'"
echo "   - 'List all my mart models'"
echo "   - 'Run dbt list to see all models'"
