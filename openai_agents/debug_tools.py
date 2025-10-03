#!/usr/bin/env python3
"""
Debug script to see what's actually happening with dbt tools.
This will help us understand why the agent isn't getting real data.
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
        return True
    return False

async def debug_tool_execution():
    """Debug what happens when we try to use dbt tools."""
    
    if not load_environment():
        print("❌ Environment not loaded")
        return False
    
    print("🔍 Debugging dbt tool execution...")
    print(f"📁 Project: {os.environ.get('DBT_PROJECT_DIR')}")
    print(f"🔧 dbt Path: {os.environ.get('DBT_PATH')}")
    print()
    
    try:
        # Configure MCP server
        async with MCPServerStdio(
            name="dbt",
            params={
                "command": "/opt/homebrew/Caskroom/miniconda/base/envs/dbt_env/bin/python",
                "args": ["-m", "dbt_mcp.main"],
            },
            client_session_timeout_seconds=30,
            cache_tools_list=True,
            tool_filter=create_static_tool_filter(
                allowed_tool_names=["dbt_list"],
            ),
        ) as server:
            
            print("✅ MCP Server connected")
            
            # Create a simple agent
            agent = Agent(
                name="debug_agent",
                instructions="""You are a debug agent. When asked about models, you MUST:
1. Use the dbt_list tool
2. Show the EXACT output from the tool
3. Do not guess or make up information
4. If the tool fails, say so clearly""",
                mcp_servers=[server],
            )
            
            print("✅ Agent created")
            
            # Test with a simple query
            print("\n🧪 Testing: 'What models do I have?'")
            print("=" * 50)
            
            conversation = [{"role": "user", "content": "What models do I have? List them with their exact file paths."}]
            
            try:
                result = await Runner.run(agent, conversation)
                response = result.final_output
                
                print("🤖 Agent Response:")
                print("-" * 30)
                print(response)
                print("-" * 30)
                
                # Check if response contains actual dbt output
                if "models/" in response or ".sql" in response or "staging" in response or "mart" in response:
                    print("✅ SUCCESS: Agent got real dbt data")
                else:
                    print("❌ FAILURE: Agent did not get real dbt data")
                    print("Response appears to be hallucinated or template-based")
                
            except Exception as e:
                print(f"❌ Error during tool execution: {e}")
                print("This suggests the dbt tools are not working properly")
            
            return True
            
    except Exception as e:
        print(f"❌ Failed to connect to dbt MCP server: {e}")
        print("This suggests the MCP server itself is not working")
        return False

async def main():
    """Run the debug test."""
    print("🚀 Debugging dbt AI Agent Tool Execution...\n")
    
    success = await debug_tool_execution()
    
    if success:
        print("\n✅ Debug test completed!")
        print("Check the output above to see if tools are actually working.")
    else:
        print("\n❌ Debug test failed. The MCP server connection is broken.")

if __name__ == "__main__":
    asyncio.run(main())
