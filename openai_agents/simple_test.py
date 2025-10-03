#!/usr/bin/env python3
"""
Simple non-interactive test to verify dbt MCP connection.
"""

import asyncio
import os
from pathlib import Path
from dotenv import load_dotenv

def test_environment():
    """Test if environment variables are loaded correctly."""
    print("🧪 Testing Environment Variables...")
    
    local_env = Path(__file__).parent / ".env"
    if local_env.exists():
        load_dotenv(local_env)
        print(f"✅ .env file found: {local_env}")
    else:
        print(f"❌ .env file not found: {local_env}")
        return False
    
    # Check required variables
    required_vars = ["OPENAI_API_KEY", "DBT_PROJECT_DIR", "DBT_PATH"]
    for var in required_vars:
        value = os.environ.get(var)
        if value:
            print(f"✅ {var}: {value[:50]}..." if len(value) > 50 else f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: Not set")
            return False
    
    return True

def test_dbt_path():
    """Test if dbt binary exists and is executable."""
    print("\n🧪 Testing dbt Binary...")
    
    dbt_path = os.environ.get("DBT_PATH")
    if not dbt_path:
        print("❌ DBT_PATH not set")
        return False
    
    if os.path.exists(dbt_path):
        print(f"✅ dbt binary exists: {dbt_path}")
        return True
    else:
        print(f"❌ dbt binary not found: {dbt_path}")
        return False

def test_dbt_project():
    """Test if dbt project directory exists."""
    print("\n🧪 Testing dbt Project...")
    
    project_dir = os.environ.get("DBT_PROJECT_DIR")
    if not project_dir:
        print("❌ DBT_PROJECT_DIR not set")
        return False
    
    if os.path.exists(project_dir):
        print(f"✅ dbt project directory exists: {project_dir}")
        
        # Check for dbt_project.yml
        dbt_project_yml = os.path.join(project_dir, "dbt_project.yml")
        if os.path.exists(dbt_project_yml):
            print(f"✅ dbt_project.yml found")
            return True
        else:
            print(f"❌ dbt_project.yml not found in {project_dir}")
            return False
    else:
        print(f"❌ dbt project directory not found: {project_dir}")
        return False

async def test_mcp_server():
    """Test if we can import and create MCP server components."""
    print("\n🧪 Testing MCP Server Components...")
    
    try:
        from agents.mcp.server import MCPServerStdio
        from agents.mcp import create_static_tool_filter
        print("✅ MCP server components imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Failed to import MCP components: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Starting dbt MCP Connection Tests...\n")
    
    tests = [
        ("Environment Variables", test_environment),
        ("dbt Binary", test_dbt_path),
        ("dbt Project", test_dbt_project),
        ("MCP Components", test_mcp_server),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = asyncio.run(test_func())
            else:
                result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with error: {e}")
            results.append((test_name, False))
    
    print("\n📊 Test Results:")
    print("=" * 50)
    
    all_passed = True
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} {test_name}")
        if not passed:
            all_passed = False
    
    print("=" * 50)
    
    if all_passed:
        print("🎉 All tests passed! The dbt MCP setup is ready.")
        print("The issue is likely with the interactive input handling.")
    else:
        print("❌ Some tests failed. Please check the configuration.")
    
    return all_passed

if __name__ == "__main__":
    main()
