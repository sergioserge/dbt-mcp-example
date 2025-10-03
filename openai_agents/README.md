# dbt AI Agent

A simple dbt AI Agent using OpenAI Agents with local dbt MCP server. This is a proof of concept that allows you to interact with your dbt project using natural language.

## Features

- 🤖 Natural language interface for dbt operations
- 📁 Access to your local dbt project
- 🔧 Full dbt CLI tool access
- 💬 Interactive conversation
- 🛡️ All credentials stored in .env files

## Quick Start

1. **Setup the agent:**
   ```bash
   cd /Users/sergejlust/Documents/my_projects/AgenticProjects/mcp_projects/openai_agents
   ./setup.sh
   ```

2. **Add your OpenAI API key:**
   - Edit `.env` file
   - Replace `your_openai_api_key_here` with your actual API key
   - Get your API key from: https://platform.openai.com/api-keys

3. **Run the agent:**
   ```bash
   python dbt_ai_agent.py
   ```

## Configuration

All configuration is handled through `.env` files:

- **Local .env**: Contains OpenAI API key and any local overrides
- **dbt-mcp/.env**: Contains dbt project configuration (already set up)

### Environment Variables

- `OPENAI_API_KEY`: Your OpenAI API key (required)
- `DBT_PROJECT_DIR`: Path to your dbt project (from dbt-mcp/.env)
- `DBT_PATH`: Path to dbt binary (from dbt-mcp/.env)
- `DBT_CLI_TIMEOUT`: Timeout for dbt commands (optional)

## Example Questions

- "What models do I have in my dbt project?"
- "Show me the structure of my staging models"
- "List all my mart models"
- "Run dbt list to see all models"
- "Help me create a new model for customer analytics"
- "What are the relationships between my models?"

## Troubleshooting

- Make sure your conda environment `dbt_env` is activated
- Check that all paths in `.env` files are correct
- Ensure your OpenAI API key is valid and has credits
- Verify that your dbt project is accessible

## Next Steps

This is a proof of concept. You can enhance it by:
- Adding more sophisticated prompts
- Implementing model creation capabilities
- Adding code generation features
- Integrating with your development workflow
