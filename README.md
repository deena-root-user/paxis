# Paxis 🕷️

Paxis is an elite, autonomous pentesting and bug-hunting agent designed and maintained by **Deena Dayalan**. 

Powered by local AI models (via Ollama) and a robust Python backend, Paxis acts as an intelligent, self-driving security tool. It doesn't just run static scans; it actively reasons, plans, executes commands, and learns from target environments using its "Deep Memory Target State."

## 🚀 Features

- **Autonomous AI Mode:** Provide an objective, and Paxis will autonomously run commands, analyze output, self-correct, and pivot until the objective is achieved.
- **Advanced Vulnerability Scanning:** Built-in native detection for:
  - SQL Injection (SQLi)
  - Cross-Site Scripting (XSS)
  - Local File Inclusion (LFI)
  - Server-Side Template Injection (SSTI)
  - Server-Side Request Forgery (SSRF)
  - OS Command Injection
- **Web Crawling & Secret Extraction:** Automatically crawls targets to discover endpoints, JavaScript files, and extract hardcoded secrets (API keys, JWTs, AWS keys).
- **Fast Directory & Port Scanning:** Multi-threaded native directory brute-forcing and port scanning.
- **Auto-Exploitation Wrapper:** Seamlessly wraps and executes external tools like `sqlmap`, automatically installing missing dependencies if needed.
- **Deep Memory State Tracking:** Paxis persistently logs discovered subdomains, open ports, vulnerabilities, and credentials to a local JSON state tracker, ensuring it retains context across long engagements.
- **MCP Tool Integration:** Expandable capabilities via the Model Context Protocol (MCP).
- **Web-Based Interface:** Clean, chat-based Flask UI to interact with your autonomous agent, manage projects, and view scan reports.

## 🛠️ How It Works

Paxis utilizes a powerful loop of execution driven by a local LLM:
1. **System Prompt Injection:** Paxis generates a dynamically updated system prompt containing the current target state (Deep Memory) and available tools.
2. **Reasoning & Planning:** The agent writes out its thought process, critiques past actions to avoid loops or WAF blocks, and formulates a plan.
3. **Execution:** The agent issues specific tags (e.g., `<<EXECUTE>>`, `<<PORT_SCAN>>`, `<<CRAWL>>`) which the backend intercepts and runs natively on the host or against the target.
4. **Analysis:** Command outputs are summarized using the LLM to filter noise and extract actionable intelligence.
5. **Memory Update:** Findings are pushed to Deep Memory (`<<UPDATE_STATE>>`), and the loop continues until the bug is found.

## 💻 Installation & Setup

### Prerequisites
- Python 3
- [Ollama](https://ollama.com/) running locally with a model installed (e.g., `llama3`).

### Installation
1. Clone the repository and navigate to the project directory:
   ```bash
   cd paxis
   ```
2. Install the required Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Ensure Ollama is running in the background and your preferred model is pulled:
   ```bash
   ollama run llama3
   ```

## 🎯 How to Run
1. Start the Paxis server:
   ```bash
   python paxis.py
   ```
2. Open your web browser and navigate to:
   ```
   http://127.0.0.1:5000
   ```
3. Create a new project in the web UI, type your objective (e.g., "Find vulnerabilities on http://example.com"), enable Autonomous Mode, and let Paxis do the heavy lifting!

## ⚠️ Disclaimer
This tool is for educational and authorized security testing purposes only. Do not use Paxis against targets you do not have explicit permission to test.
