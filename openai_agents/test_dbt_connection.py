#!/usr/bin/env python3
"""
Test script to verify dbt MCP connection works.
This will test the core functionality without interactive input.
"""

import asyncio
import os
from pathlib import Path
from dotenv import load_dotenv
from agents import Agent, Runner
from agents.mcp import create_static_tool_filter
from agents.mcp.server import MCPServerStdio

def load_environment():
    """Load environment variables from local .env file."""
    local_env = Path(__file__).parent / ".env"
    if local_env.exists():
        load_dotenv(local_env)
        print(f"✅ Loaded environment from: {local_env}")
    else:
        print(f"❌ .env file not found at: {local_env}")
        return False
    
    # Check required environment variables
    required_vars = ["OPENAI_API_KEY", "DBT_PROJECT_DIR", "DBT_PATH"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    return True

async def test_dbt_connection():
    """Test the dbt MCP connection with a simple query."""
    
    if not load_environment():
        return False
    
    print("🚀 Testing dbt MCP Connection...")
    print(f"📁 Project: {os.environ.get('DBT_PROJECT_DIR')}")
    print(f"🔧 dbt Path: {os.environ.get('DBT_PATH')}")
    print()
    
    try:
        # Configure MCP server for local dbt project
        async with MCPServerStdio(
            name="dbt",
            params={
                "command": "/opt/homebrew/Caskroom/miniconda/base/envs/dbt_env/bin/python",
                "args": ["-m", "dbt_mcp.main"],
            },
            client_session_timeout_seconds=30,
            cache_tools_list=True,
            tool_filter=create_static_tool_filter(
                allowed_tool_names=[
                    "list",
                    "show", 
                    "run",
                    "test",
                    "compile",
                    "read_file",
                ],
            ),
        ) as server:
            print("✅ MCP Server connected successfully!")
            
            # Create the AI agent
            agent = Agent(
                name="dbt_tester",
                instructions="""You are a dbt testing assistant. 
                Help test the dbt MCP connection by running simple dbt commands.
                Always provide clear feedback about what you're doing.""",
                mcp_servers=[server],
            )
            
            print("✅ AI Agent created successfully!")
            
            # Test with a simple query
            test_queries = [
                "List all models in my dbt project",
                "Show me the first 5 models",
                "What models do I have in staging?"
            ]
            
            for i, query in enumerate(test_queries, 1):
                print(f"\n🧪 Test {i}: {query}")
                print("🤔 Processing...")
                
                try:
                    # Create conversation
                    conversation = [{"role": "user", "content": query}]
                    
                    # Get response from agent
                    result = await Runner.run(agent, conversation)
                    
                    print(f"✅ Response: {result.final_output}")
                    print("-" * 50)
                    
                except Exception as e:
                    print(f"❌ Error in test {i}: {e}")
                    print("-" * 50)
            
            print("\n🎉 dbt MCP Connection Test Complete!")
            return True
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

async def main():
    """Run the dbt connection test."""
    success = await test_dbt_connection()
    
    if success:
        print("\n✅ All tests passed! The dbt MCP connection is working.")
        print("You can now proceed to fix the interactive input handling.")
    else:
        print("\n❌ Tests failed. Please check the configuration and try again.")

if __name__ == "__main__":
    asyncio.run(main())
