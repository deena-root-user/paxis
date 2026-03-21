import requests
import json

class MCPClient:
    def __init__(self, server_url, api_key=None):
        self.server_url = server_url.rstrip('/')
        self.api_key = api_key
        self.headers = {"Content-Type": "application/json"}
        if self.api_key:
            self.headers["Authorization"] = f"Bearer {self.api_key}"

    def get_available_tools(self):
        """
        Fetches the list of available tools from the MCP server.
        Assumes the server exposes a /tools endpoint (common convention).
        """
        try:
            response = requests.get(f"{self.server_url}/tools", headers=self.headers, timeout=5)
            response.raise_for_status()
            return response.json().get("tools", [])
        except Exception as e:
            print(f"[MCP Error] Failed to fetch tools: {e}")
            return []

    def execute_tool(self, tool_name, arguments):
        """
        Executes a tool on the MCP server.
        """
        payload = {
            "name": tool_name,
            "arguments": arguments
        }
        try:
            response = requests.post(f"{self.server_url}/tools/execute", headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            return response.json().get("result", "")
        except Exception as e:
            return f"[MCP Error] Tool execution failed: {e}"

    def format_tools_for_prompt(self, tools):
        """
        Formats the tool definitions into a string for the system prompt.
        """
        if not tools:
            return ""
        
        prompt = "\nAvailable MCP Tools:\n"
        for tool in tools:
            prompt += f"- {tool.get('name')}: {tool.get('description')}\n"
            prompt += f"  Args: {json.dumps(tool.get('inputSchema'))}\n"
        
        prompt += "\nTo use a tool, output: <<MCP:tool_name(json_args)>>\n"
        return prompt
