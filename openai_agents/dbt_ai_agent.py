#!/usr/bin/env python3
"""
Improved dbt AI Agent with tool-first approach and better reliability.
This version focuses on using dbt tools first and reducing hallucination.
"""

import asyncio
import json
import os
import sys
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

def get_user_input(prompt):
    """Get user input with better error handling."""
    try:
        return input(prompt).strip()
    except (EOFError, KeyboardInterrupt):
        return None
    except Exception as e:
        print(f"Input error: {e}")
        return None

def load_instructions(approach="principles_based1"):
    """Load instructions from JSON file."""
    instructions_file = Path(__file__).parent / "instructions.json"
    try:
        with open(instructions_file, 'r') as f:
            instructions_data = json.load(f)
        return instructions_data.get(approach, instructions_data["principles_based1"])
    except Exception as e:
        print(f"❌ Error loading instructions: {e}")
        return "You are a dbt assistant that uses dbt tools first."

async def main():
    """Start a conversation with the improved dbt AI agent."""
    
    # Load environment variables
    if not load_environment():
        return
    
    print("🚀 Starting Improved dbt AI Agent...")
    print(f"📁 Project: {os.environ.get('DBT_PROJECT_DIR')}")
    print(f"🔧 dbt Path: {os.environ.get('DBT_PATH')}")
    print("💬 Type 'quit' to exit\n")
    
    try:
        # Configure MCP server for local dbt project
        async with MCPServerStdio(
            name="dbt",
            params={
                "command": "/opt/homebrew/Caskroom/miniconda/base/envs/dbt_env/bin/python",
                "args": ["-m", "dbt_mcp.main"],
            },
            client_session_timeout_seconds=int(os.environ.get("DBT_CLI_TIMEOUT", "30")),
            cache_tools_list=True,
            tool_filter=create_static_tool_filter(
                allowed_tool_names=[
                    # dbt CLI tools - prioritized list
                    "list",
                    "show", 
                    "run",
                    "test",
                    "compile",
                    "parse",
                    "docs",
                    "build",
                    "read_file",
                ],
            ),
        ) as server:
            print("✅ Connected to dbt MCP server!")
            
            # Load instructions from JSON file
            instructions = load_instructions("reasoning_based1")  # Change to "reasoning_based2" to test the other approach
            
            # Create the improved AI agent with tool-first instructions
            agent = Agent(
                name="dbt_assistant",
                model="gpt-4o-mini",  # Cost-effective model for dbt tasks
                instructions=instructions,
                mcp_servers=[server],
            )
            
            print("✅ Improved AI Agent ready!")
            print("\n" + "="*60)
            print("🤖 dbt Assistant (Tool-First Mode)")
            print("🔧 Always uses dbt tools before responding")
            print("📊 Shows actual dbt output")
            print("❌ Never guesses or hallucinates")
            print("="*60)
            
            # Start conversation loop
            conversation = []
            while True:
                try:
                    user_input = get_user_input("\n🤖 You > ")
                    
                    if user_input is None:
                        print("\n👋 Goodbye!")
                        break
                    
                    if user_input.lower() in ['quit', 'exit', 'bye']:
                        print("👋 Goodbye!")
                        break
                    
                    if not user_input:
                        continue
                    
                    # Add user message to conversation
                    conversation.append({"role": "user", "content": user_input})
                    
                    print("🤔 Thinking... (Using dbt tools first)")
                    
                    try:
                        # Get response from agent
                        result = await Runner.run(agent, conversation)
                        
                        print(f"\n🤖 dbt Assistant > {result.final_output}")
                        
                        # Update conversation with assistant response
                        conversation.append({"role": "assistant", "content": result.final_output})
                        
                    except Exception as e:
                        print(f"❌ Error processing request: {e}")
                        print("Please try again or type 'quit' to exit.")
                        # Remove the last user message if there was an error
                        if conversation and conversation[-1]["role"] == "user":
                            conversation.pop()
                
                except KeyboardInterrupt:
                    print("\n👋 Goodbye!")
                    break
                except Exception as e:
                    print(f"❌ Unexpected error: {e}")
                    print("Please try again or type 'quit' to exit.")
    
    except Exception as e:
        print(f"❌ Failed to start dbt MCP server: {e}")
        print("Please check your configuration and try again.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)
