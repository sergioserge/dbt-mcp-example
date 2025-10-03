#!/usr/bin/env python3
"""
Test script to validate the improved dbt AI agent.
This tests the tool-first approach and reliability improvements.
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

async def test_tool_first_approach():
    """Test if the agent uses tools first instead of hallucinating."""
    
    if not load_environment():
        print("❌ Environment not loaded")
        return False
    
    print("🧪 Testing Tool-First Approach...")
    
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
                allowed_tool_names=["dbt_list", "dbt_show"],
            ),
        ) as server:
            
            # Create agent with tool-first instructions
            agent = Agent(
                name="dbt_tester",
                instructions="""You are a dbt assistant that ALWAYS uses dbt tools first.

CRITICAL RULES:
1. NEVER guess or assume - always use dbt tools first
2. ALWAYS run the appropriate dbt command before answering
3. Show the user which dbt command you're running
4. Present the actual dbt output alongside your interpretation
5. If a dbt command fails, ask the user to retry instead of guessing

RESPONSE FORMAT:
1. State which dbt command you're running
2. Show the actual dbt output
3. Provide your interpretation based on the actual data
4. If no data is available, say "I don't know" instead of guessing""",
                mcp_servers=[server],
            )
            
            # Test queries
            test_queries = [
                "What models do I have?",
                "List my staging models",
                "Show me the first 5 models"
            ]
            
            for i, query in enumerate(test_queries, 1):
                print(f"\n🧪 Test {i}: {query}")
                print("🤔 Processing...")
                
                try:
                    conversation = [{"role": "user", "content": query}]
                    result = await Runner.run(agent, conversation)
                    
                    response = result.final_output
                    
                    # Check if response mentions tool usage
                    if "dbt_list" in response or "dbt show" in response or "running" in response.lower():
                        print(f"✅ PASS: Agent used tools first")
                        print(f"Response: {response[:100]}...")
                    else:
                        print(f"❌ FAIL: Agent may have hallucinated")
                        print(f"Response: {response[:100]}...")
                    
                    print("-" * 50)
                    
                except Exception as e:
                    print(f"❌ Error in test {i}: {e}")
                    print("-" * 50)
            
            return True
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

async def main():
    """Run the improvement tests."""
    print("🚀 Testing dbt AI Agent Improvements...\n")
    
    success = await test_tool_first_approach()
    
    if success:
        print("\n✅ Tool-first approach test completed!")
        print("Check the responses above to see if the agent is using tools first.")
    else:
        print("\n❌ Tests failed. Please check the configuration.")

if __name__ == "__main__":
    asyncio.run(main())
