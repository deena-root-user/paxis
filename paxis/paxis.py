#!/usr/bin/env python3
"""
Paxis
---------------------------
Designed and maintained by IHA089.
"""

import warnings
warnings.filterwarnings("ignore")
import sys
sys.modules['warnings'] = warnings

import queue
import threading
import subprocess, json, re, os, sqlite3, hashlib, uuid, secrets, requests, shlex, socket
from concurrent.futures import ThreadPoolExecutor
from mcp_client import MCPClient
from waitress import serve
from flask import Flask, request, jsonify, render_template, Response, stream_with_context, make_response, send_from_directory
from werkzeug.utils import secure_filename 

# Updater removed by user request
# try:
#     from updater import update_paxis
#     update_paxis()
# except Exception as e:
#     print(f"[Update Check Failed] {e}")


import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("paxis.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("paxis")

paxis = Flask(__name__)
DB_NAME = 'chat_database.db'
UPLOAD_FOLDER = 'uploads' 
CONFIG_FILE = 'config.json'
paxis.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def load_config():
    if not os.path.exists(CONFIG_FILE):
        default_config = {
            "kali_server_url": "",
            "kali_api_key": "",
            "enable_kali_integration": False,
            "mcp_server_url": "",
            "mcp_api_key": "",
            "enable_mcp_integration": False,
            "enable_autonomous_mode": False
        }
        with open(CONFIG_FILE, 'w') as f:
            json.dump(default_config, f, indent=4)
        return default_config
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return {}

def save_config(config_data):
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config_data, f, indent=4)
        return True
    except Exception as e:
        logger.error(f"Error saving config: {e}")
        return False

def init_db():
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("PRAGMA foreign_keys = ON;")
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_hash TEXT PRIMARY KEY,
            username TEXT NOT NULL
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            project_id TEXT PRIMARY KEY,
            user_hash TEXT NOT NULL,
            title TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_hash) REFERENCES users(user_hash) ON DELETE CASCADE
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS chats (
            chat_id TEXT PRIMARY KEY,
            user_hash TEXT NOT NULL,
            title TEXT NOT NULL,
            model_name TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            project_id TEXT,
            FOREIGN KEY(user_hash) REFERENCES users(user_hash) ON DELETE CASCADE,
            FOREIGN KEY(project_id) REFERENCES projects(project_id) ON DELETE CASCADE
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            message_id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            sender TEXT NOT NULL,
            text TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            file_path TEXT,
            file_name TEXT,
            FOREIGN KEY(chat_id) REFERENCES chats(chat_id) ON DELETE CASCADE
        )
    ''')
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS command_outputs (
            output_id TEXT PRIMARY KEY,
            chat_id TEXT NOT NULL,
            command TEXT NOT NULL,
            output TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(chat_id) REFERENCES chats(chat_id) ON DELETE CASCADE
        )
    ''')

    try:
        c.execute("ALTER TABLE chats ADD COLUMN model_name TEXT NOT NULL DEFAULT 'llama3'")
    except sqlite3.OperationalError:
        pass
        
    try:
        c.execute("ALTER TABLE messages ADD COLUMN file_path TEXT")
        c.execute("ALTER TABLE messages ADD COLUMN file_name TEXT")
    except sqlite3.OperationalError:
        pass 
        
    try:
        c.execute("ALTER TABLE messages ADD COLUMN summary TEXT")
    except sqlite3.OperationalError:
        pass
        
    try:
        c.execute("ALTER TABLE chats ADD COLUMN project_id TEXT REFERENCES projects(project_id) ON DELETE CASCADE")
    except sqlite3.OperationalError:
        pass 

    conn.commit()
    conn.close()


def summarize_tool_output(model_name, command, raw_output):
    """Deterministically truncate and intelligently summarize raw tool output."""
    if len(raw_output) > 100000:
        raw_output = raw_output[:50000] + "\n\n...[Output Truncated]...\n\n" + raw_output[-50000:]
        
    prompt = f"The user ran the command: `{command}`\nHere is the raw output:\n```\n{raw_output}\n```\nExtract ONLY the most critical findings, credentials, open ports, vulnerabilities, or errors. Ignore noise. Format your output as a concise bulleted list or a brief JSON object."
    
    ollama_url = "http://localhost:11434/api/generate"
    payload = {
        "model": model_name,
        "prompt": prompt,
        "stream": False,
        "options": {
            "num_predict": 2048,
            "num_ctx": 32768
        }
    }
    try:
        r = requests.post(ollama_url, json=payload, timeout=60)
        r.raise_for_status()
        return r.json().get("response", raw_output[:1000] + " [Failed to summarize]")
    except Exception as e:
        logger.error(f"[Ollama Summarize Error] {e}")
        return raw_output[:1000] + f" [Summarization error: {e}]"

def get_chat_history_for_ollama(chat_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    try:
        c.execute("SELECT sender, text, file_name, summary FROM messages WHERE chat_id = ? ORDER BY timestamp ASC", (chat_id,))
        rows = c.fetchall()
    except sqlite3.OperationalError:
        # Fallback if summary column isn't fully migrated yet
        c.execute("SELECT sender, text, file_name FROM messages WHERE chat_id = ? ORDER BY timestamp ASC", (chat_id,))
        rows = [(r[0], r[1], r[2], None) for r in c.fetchall()]

    history = []
    for row in rows:
        sender, text, file_name, summary = row
        role = 'user' if sender == 'user' else 'assistant'
        
        # Output Filtering layer: Use compressed summary signal over raw text for agent's own history
        content = summary if summary else text
        if len(content) > 30000:
            content = content[:15000] + "\n\n...[Output Truncated]...\n\n" + content[-15000:]
            
        if file_name:
            content = f"(The user has attached a file: {file_name})\n\n{content}"
        history.append({'role': role, 'content': content})
    conn.close()
    return history

def stream_ollama_response(model_name, history, new_message, chat_id, config):
    ollama_url = "http://localhost:11434/api/chat"
    
    # Initialize MCP Client if enabled
    mcp_tools = []
    mcp_client = None
    if config.get('enable_mcp_integration') and config.get('mcp_server_url'):
        mcp_client = MCPClient(config.get('mcp_server_url'), config.get('mcp_api_key'))
        mcp_tools = mcp_client.get_available_tools()

    # Inject System Prompt for Autonomous Mode & MCP
    system_prompt_parts = []
    
    if config.get('enable_autonomous_mode'):
        system_prompt_parts.append(
            "You are Paxis, an elite autonomous pentesting agent. Your primary goal is to find vulnerabilities and achieve the objective set by the user. You have the ability to execute system commands on the host machine and interact with web services.\n"
            "Your behavior rules:\n"
            "1. ALWAYS structure your response strictly using these Markdown headers to enforce sequential reasoning:\n"
            "   ### Reasoning\n"
            "   (Explain your thought process, what you deduce from the previous output, and how it relates to the overall objective. Be concise but thorough.)\n"
            "   ### Critique\n"
            "   (Identify any flaws in your previous logic, potential loops, false positives, or missed opportunities. Course-correct if needed. Consider WAF/IDS evasion techniques if encountering resistance.)\n"
            "   ### Plan\n"
            "   (State your next short-term action steps safely without restating the long-term plan. Prioritize reconnaissance, then vulnerability identification, then exploitation. Think about chaining commands.)\n"
            "   ### Command\n"
            "   (If you need to execute a command, MUST use EXACTLY this format: <<EXECUTE>> your_command_here <<END>>)\n"
            "   (To read a file safely in segments, MUST use EXACTLY this format: <<READ_FILE_CHUNK>> path offset_lines limit_lines <<END>>)\n"
            "   (To run a fast asynchronous port scan, MUST use EXACTLY this format: <<PORT_SCAN>> host start_port-end_port <<END>>)\n"
            "   (To scrape endpoints and forms from a URL, MUST use EXACTLY this format: <<CRAWL>> url <<END>>)\n"
            "   (To find hidden sensitive directories like .env or admin/, MUST use EXACTLY this format: <<DIR_BRUTE>> url <<END>>)\n"
            "   (To test a URL with parameters for SQLi, XSS, and LFI natively, MUST use EXACTLY this format: <<VULN_SCAN>> url_with_params <<END>>)\n"
            "   (To launch an automated exploit tool cleanly, MUST use EXACTLY this format: <<EXPLOIT>> tool options target <<END>>)\n"
            "   (To perform OSINT on a target, MUST use EXACTLY this format: <<OSINT>> target_query <<END>>)\n"
            "   (To record a hard fact or discovery into your Deep Memory JSON, MUST use: <<UPDATE_STATE>> key value <<END>>)\n"
            "   (If you are FINISHED with the objective and exhausted all avenues, write: # Final Report \\n [Detailed markdown report here])\n"
            "2. Wait for the command output. DO NOT ask for permission to run standard commands. Assume you have permission to execute any command necessary for the pentest.\n"
            "3. Deep Memory Context: Your Target State JSON is provided in every prompt. Use <<UPDATE_STATE>> to record new subdomains, ports, credentials, discovered vulnerabilities, or interesting findings so you don't forget them. This is crucial for maintaining state.\n"
            "4. Missing Tools: If a command or tool fails because it is not installed/not found, your NEXT Plan MUST be to execute commands to install it (e.g. using pip, npm, apt-get, etc.) before proceeding. Always try to install missing tools.\n"
            "5. The system will auto-feed summarized command outputs to you. Keep engaging. Never give up until you find the bug or achieve the objective. Prioritize stealth and WAF evasion where applicable.\n"
            "6. If you encounter a WAF or other security measures, try alternative methods, encoding, or different tools. Document your evasion attempts in your Critique.\n"
        )
    
    if mcp_tools and mcp_client:
        system_prompt_parts.append(mcp_client.format_tools_for_prompt(mcp_tools))
        
    if system_prompt_parts:
        system_prompt = "\n\n".join(system_prompt_parts)
        
        # Inject Target State deep memory into the system prompt dynamic payload
        state_file_path = os.path.join(paxis.config['UPLOAD_FOLDER'], f"state_{chat_id}.json")
        target_state = {}
        if os.path.exists(state_file_path):
            try:
                with open(state_file_path, 'r', encoding='utf-8') as f:
                    target_state = json.load(f)
            except:
                pass
        system_prompt += f"\n\n--- CURRENT DEEP MEMORY TARGET STATE ---\n{json.dumps(target_state, indent=2)}\n----------------------------------------\n"
        
        # Check if system prompt already exists in history
        if history and history[0]['role'] == 'system':
            history[0]['content'] = system_prompt
        else:
            history.insert(0, {'role': 'system', 'content': system_prompt})
    
    messages = history 
    
    max_loops = 1000 
    loop_count = 0
    
    while loop_count < max_loops:
        loop_count += 1
        
        payload = {
            "model": model_name,
            "messages": messages,
            "stream": True,
            "options": {
                "num_ctx": 32768,
                "num_predict": 4096
            }
        }
        
        ai_full_response = ""
        
        # We use a Queue and a backend thread to keep yielding heartbeats to Cloudflare
        # while waiting for Ollama to process the prompt and start streaming.
        q = queue.Queue()
        
        def fetch_ollama():
            try:
                with requests.post(ollama_url, json=payload, stream=True) as r:
                    r.raise_for_status()
                    for line in r.iter_lines():
                        if line:
                            data = json.loads(line.decode("utf-8"))
                            if "content" in data["message"]:
                                chunk = data["message"]["content"]
                                q.put(("chunk", chunk))
                            
                            if data.get("done"):
                                q.put(("done", None))
                                break
            except Exception as e:
                q.put(("error", e))
                
        threading.Thread(target=fetch_ollama, daemon=True).start()
        
        try:
            while True:
                try:
                    # Wait up to 15 seconds for a chunk from Ollama
                    msg_type, data = q.get(timeout=15)
                    
                    if msg_type == "chunk":
                        ai_full_response += data
                        yield data
                    elif msg_type == "done":
                        break
                    elif msg_type == "error":
                        logger.error(f"[Ollama Stream Error] {data}")
                        yield f"\n[Error accessing Ollama: {data}]\n"
                        return
                except queue.Empty:
                    # Timeout reached without data, yield a space to keep proxy connection alive
                    yield " "
        except Exception as e:
            logger.error(f"[Ollama Stream Loop Error] {e}")
            return
        
        # Save AI response to DB
        if ai_full_response:
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT INTO messages (chat_id, sender, text, summary) VALUES (?, ?, ?, ?)", (chat_id, 'ai', ai_full_response, ai_full_response))
            conn.commit()
            conn.close()

        # Check for Command Execution (Autonomous Mode)
        cmd_match = re.search(r'<<EXECUTE>>(.*?)<<END>>', ai_full_response, re.DOTALL)
        
        # Check for File Chunk Reading
        read_match = re.search(r'<<READ_FILE_CHUNK>>(.*?)<<END>>', ai_full_response, re.DOTALL)
        
        # Check for Native Port Scan
        scan_match = re.search(r'<<PORT_SCAN>>(.*?)<<END>>', ai_full_response, re.DOTALL)
        
        # Check for Deep Memory State Update
        state_match = re.search(r'<<UPDATE_STATE>>(.*?)<<END>>', ai_full_response, re.DOTALL)
        
        # Check for Web Crawl
        crawl_match = re.search(r'<<CRAWL>>(.*?)<<END>>', ai_full_response, re.DOTALL)
        
        # Check for Auto-Exploit
        exploit_match = re.search(r'<<EXPLOIT>>(.*?)<<END>>', ai_full_response, re.DOTALL)
        
        # Check for Native Dir Brute
        dir_match = re.search(r'<<DIR_BRUTE>>(.*?)<<END>>', ai_full_response, re.DOTALL)
        
        # Check for Native Vuln Scan
        vuln_match = re.search(r'<<VULN_SCAN>>(.*?)<<END>>', ai_full_response, re.DOTALL)
        
        # Check for Subdomain Enumeration
        subenum_match = re.search(r'<<SUB_ENUM>>(.*?)<<END>>', ai_full_response, re.DOTALL)
        
        # Check for OSINT lookup
        osint_match = re.search(r'<<OSINT>>(.*?)<<END>>', ai_full_response, re.DOTALL)
        
        # Check for MCP Tool Execution
        mcp_match = re.search(r'<<MCP:(.*?)\((.*?)\)>>', ai_full_response, re.DOTALL)

        if cmd_match and config.get('enable_autonomous_mode'):
            command_to_execute = cmd_match.group(1).strip()
            
            yield f"\n\n*Autonomous Mode: Executing command...* `{command_to_execute}`\n"
            
            output_text = ""
            try:
                # Security Fix: shell=True enabled for full power, unrestrained agent
                process = subprocess.Popen(
                    command_to_execute,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )
                
                yield "\n```text\n"
                output_lines = []
                for line in process.stdout:
                    output_lines.append(line)
                    yield line
                
                process.wait()
                yield "\n```\n"
                
                output_text = "".join(output_lines)
            except Exception as ex:
                output_text = f"Error executing command: {ex}"
                yield f"\n```text\n{output_text}\n```\n"
            
            if len(output_text) > 30000:
                output_text = output_text[:15000] + "\n\n...[Output Truncated]...\n\n" + output_text[-15000:]
                
            if "not recognized as an internal or external command" in output_text or "command not found" in output_text.lower():
                output_text += "\n\n[SYSTEM HINT]: The command or tool was not found! Review your environment and execute commands to install it before trying again."
            
            result_message = f"Command Output:\n```\n{output_text}\n```\n"
            
            summary_text = summarize_tool_output(model_name, command_to_execute, output_text)
            summary_message = f"Command Output Summary (`{command_to_execute}`):\n{summary_text}\n"
            
            messages.append({'role': 'assistant', 'content': ai_full_response})
            messages.append({'role': 'user', 'content': summary_message})
            
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT INTO messages (chat_id, sender, text, summary) VALUES (?, ?, ?, ?)", (chat_id, 'system', result_message, summary_message))
            conn.commit()
            conn.close()
            
        elif read_match and config.get('enable_autonomous_mode'):
            read_args = read_match.group(1).strip().split()
            if len(read_args) >= 3:
                file_path = read_args[0]
                try:
                    offset = int(read_args[1])
                    limit = int(read_args[2])
                    
                    yield f"\n\n*Autonomous Mode: Reading chunk of `{file_path}`...*\n"
                    
                    if not os.path.exists(file_path):
                        output_text = f"Error: File '{file_path}' does not exist."
                    elif not os.path.isfile(file_path):
                        output_text = f"Error: '{file_path}' is not a file."
                    else:
                        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                            lines = f.readlines()
                            total_lines = len(lines)
                            end_line = min(offset + limit, total_lines)
                            chunk_lines = lines[offset:end_line]
                            output_text = "".join(chunk_lines)
                            output_text = f"--- FILE: {file_path} (Lines {offset} to {end_line-1} of {total_lines}) ---\n" + output_text
                except Exception as ex:
                    output_text = f"Error reading file syntax: {ex}. Expected: <<READ_FILE_CHUNK>> path offset limit <<END>>"
            else:
                output_text = "Error: Invalid READ_FILE_CHUNK syntax. Expected: <<READ_FILE_CHUNK>> path offset limit <<END>>"
                
            result_message = f"File Read Output:\n```\n{output_text}\n```\n"
            yield f"\n```\n{output_text}\n```\n"
            
            summary_message = f"File Read Chunk: `{read_args[0] if len(read_args)>0 else 'unknown'}`\n*Read returned {len(output_text)} characters.*"
            if "Error" in output_text:
                 summary_message = output_text
            else:
                 summary_message += f"\n\nRaw Chunk Content:\n```\n{output_text[:4000]}...\n```"
                 
            messages.append({'role': 'assistant', 'content': ai_full_response})
            messages.append({'role': 'user', 'content': summary_message})
            
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT INTO messages (chat_id, sender, text, summary) VALUES (?, ?, ?, ?)", (chat_id, 'system', result_message, summary_message))
            conn.commit()
            conn.close()
            
        elif scan_match and config.get('enable_autonomous_mode'):
            scan_args = scan_match.group(1).strip().split()
            if len(scan_args) >= 2:
                target_host = scan_args[0]
                port_range_str = scan_args[1]
                
                yield f"\n\n*Autonomous Mode: Native Port Scan on `{target_host}:{port_range_str}`...*\n"
                
                try:
                    start_port, end_port = map(int, port_range_str.split('-'))
                    open_ports = []
                    
                    def check_port(port):
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(0.5)
                        result = sock.connect_ex((target_host, port))
                        sock.close()
                        if result == 0:
                            return port
                        return None

                    with ThreadPoolExecutor(max_workers=50) as executor:
                        results = executor.map(check_port, range(start_port, end_port + 1))
                        
                    open_ports = [p for p in results if p is not None]
                    
                    if open_ports:
                        output_text = f"Native Port Scan completed.\nHost: {target_host}\nOpen Ports: {open_ports}"
                    else:
                        output_text = f"Native Port Scan completed.\nHost: {target_host}\nNo open ports found in range {start_port}-{end_port}."
                        
                except Exception as ex:
                    output_text = f"Error in port scan syntax or execution: {ex}. Expected: <<PORT_SCAN>> host start_port-end_port <<END>>"
            else:
                output_text = "Error: Invalid PORT_SCAN syntax. Expected: <<PORT_SCAN>> host start_port-end_port <<END>>"
                
            result_message = f"Native Port Scan Output:\n```\n{output_text}\n```\n"
            yield f"\n```\n{output_text}\n```\n"
            
            summary_message = f"Native Port Scan (`{scan_args[0] if len(scan_args)>0 else 'unknown'}`): {output_text}"
                 
            messages.append({'role': 'assistant', 'content': ai_full_response})
            messages.append({'role': 'user', 'content': summary_message})
            
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT INTO messages (chat_id, sender, text, summary) VALUES (?, ?, ?, ?)", (chat_id, 'system', result_message, summary_message))
            conn.commit()
            conn.close()
            
        elif state_match and config.get('enable_autonomous_mode'):
            state_args = state_match.group(1).strip().split(' ', 1)
            if len(state_args) == 2:
                key, value = state_args
                
                yield f"\n\n*Autonomous Mode: Updating Deep Memory (`{key}` = `{value[:30]}...`)*\n"
                
                state_file_path = os.path.join(paxis.config['UPLOAD_FOLDER'], f"state_{chat_id}.json")
                target_state = {}
                if os.path.exists(state_file_path):
                    try:
                        with open(state_file_path, 'r', encoding='utf-8') as f:
                            target_state = json.load(f)
                    except:
                        pass
                
                target_state[key] = value
                
                try:
                    with open(state_file_path, 'w', encoding='utf-8') as f:
                        json.dump(target_state, f, indent=2)
                    output_text = f"Deep Memory Updated successfully. {key}: {value}"
                except Exception as ex:
                    output_text = f"Error updating memory state file: {ex}"
                
            else:
                output_text = "Error: Invalid UPDATE_STATE syntax. Expected: <<UPDATE_STATE>> property value <<END>>"
                
            result_message = f"State Update Output:\n```\n{output_text}\n```\n"
            yield f"\n```\n{output_text}\n```\n"
            
            messages.append({'role': 'assistant', 'content': ai_full_response})
            messages.append({'role': 'user', 'content': result_message})
            
            # Since the state is now in the persistent JSON memory, we can dynamically reload the system prompt next tick
            # Reconstruct system context:
            if history and history[0]['role'] == 'system':
                 new_system = "\n\n".join(system_prompt_parts) + f"\n\n--- CURRENT DEEP MEMORY TARGET STATE ---\n{json.dumps(target_state, indent=2)}\n----------------------------------------\n"
                 history[0]['content'] = new_system
            
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT INTO messages (chat_id, sender, text, summary) VALUES (?, ?, ?, ?)", (chat_id, 'system', result_message, result_message))
            conn.commit()
            conn.close()

        elif crawl_match and config.get('enable_autonomous_mode'):
            target_url = crawl_match.group(1).strip()
            yield f"\n\n*Autonomous Mode: Native Web Crawl on `{target_url}`...*\n"
            
            try:
                if not target_url.startswith('http'):
                    target_url = 'http://' + target_url
                from urllib.parse import urljoin
                resp = requests.get(target_url, timeout=10, verify=False)
                html = resp.text
                
                links = list(set(re.findall(r'href=[\'"]?([^\'" >]+)', html)))
                scripts = list(set(re.findall(r'src=[\'"]?([^\'" >]+)', html)))
                forms = list(set(re.findall(r'action=[\'"]?([^\'" >]+)', html)))
                
                all_links = links + scripts + forms
                full_urls = [urljoin(target_url, l) for l in all_links if not l.startswith(('javascript:', 'mailto:', '#'))]
                
                # Secret Extraction from HTML/JS source
                secret_patterns = {
                    'AWS Access Key': r'AKIA[0-9A-Z]{16}',
                    'JWT Token': r'eyJ[a-zA-Z0-9_-]{10,}\.eyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}',
                    'Generic API Key': r'(?i)(api[_-]?key|apikey|api[_-]?secret|auth[_-]?token|access[_-]?token)[\\s]*[=:][\\s]*[\'"]([a-zA-Z0-9\\-_]{20,})[\'"]',
                    'Private Key Header': r'-----BEGIN (RSA|EC|OPENSSH) PRIVATE KEY-----',
                    'Google API Key': r'AIza[0-9A-Za-z\-_]{35}',
                    'GitHub Token': r'ghp_[0-9a-zA-Z]{36}',
                    'Slack Token': r'xox[baprs]-[0-9a-zA-Z\-]+',
                }
                secrets_found = []
                for secret_type, pattern in secret_patterns.items():
                    matches = re.findall(pattern, html)
                    if matches:
                        for m in matches:
                            val = m if isinstance(m, str) else m[-1]
                            secrets_found.append(f"[CRITICAL] {secret_type}: {val[:80]}")
                
                output_text = f"Crawl successful. Discovered {len(full_urls)} endpoints.\n\nEndpoints:\n" + "\n".join(full_urls[:50])
                if len(full_urls) > 50:
                    output_text += f"\n...and {len(full_urls)-50} more."
                if secrets_found:
                    output_text += "\n\n\U0001f534 SECRETS DETECTED IN PAGE SOURCE:\n" + "\n".join(secrets_found)
                else:
                    output_text += "\n\nNo common hardcoded secrets found in page source."
            except Exception as ex:
                output_text = f"Error during crawl: {ex}"
                
            result_message = f"Crawl Output:\n```\n{output_text}\n```\n"
            yield f"\n```\n{output_text}\n```\n"
            
            summary_message = f"Web Crawl (`{target_url}`): {output_text}"
            
            messages.append({'role': 'assistant', 'content': ai_full_response})
            messages.append({'role': 'user', 'content': summary_message})
            
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT INTO messages (chat_id, sender, text, summary) VALUES (?, ?, ?, ?)", (chat_id, 'system', result_message, summary_message))
            conn.commit()
            conn.close()

        elif exploit_match and config.get('enable_autonomous_mode'):
            exploit_cmd = exploit_match.group(1).strip()
            yield f"\n\n*Autonomous Mode: Running Auto-Exploit Wrapper for `{exploit_cmd}`...*\n"
            
            try:
                if "sqlmap" in exploit_cmd.lower() and "--batch" not in exploit_cmd.lower():
                    exploit_cmd += " --batch"
                    
                process = subprocess.Popen(
                    exploit_cmd,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )
                
                yield "\n```text\n"
                output_lines = []
                for line in process.stdout:
                    output_lines.append(line)
                    yield line
                
                process.wait()
                yield "\n```\n"
                
                output_text = "".join(output_lines)
            except Exception as ex:
                output_text = f"Error executing exploit tool: {ex}"
                yield f"\n```text\n{output_text}\n```\n"
                
            if len(output_text) > 30000:
                output_text = output_text[:15000] + "\n\n...[Output Truncated]...\n\n" + output_text[-15000:]
                
            if "not recognized as an internal or external command" in output_text or "command not found" in output_text.lower() or "not found" in output_text.lower():
                output_text += "\n\n[SYSTEM HINT]: The exploit tool was not found on this system! Your next step MUST be to install it using <<EXECUTE>>."
                
            result_message = f"Exploit Output:\n```\n{output_text}\n```\n"
            
            summary_text = summarize_tool_output(model_name, exploit_cmd, output_text)
            summary_message = f"Exploit Output Summary (`{exploit_cmd}`):\n{summary_text}\n"
            
            messages.append({'role': 'assistant', 'content': ai_full_response})
            messages.append({'role': 'user', 'content': summary_message})
            
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT INTO messages (chat_id, sender, text, summary) VALUES (?, ?, ?, ?)", (chat_id, 'system', result_message, summary_message))
            conn.commit()
            conn.close()

        elif dir_match and config.get('enable_autonomous_mode'):
            target_url = dir_match.group(1).strip()
            if not target_url.startswith('http'):
                target_url = 'http://' + target_url
            if target_url.endswith('/'):
                target_url = target_url[:-1]
                
            yield f"\n\n*Autonomous Mode: Native Fast Directory Bruteforce on `{target_url}`...*\n"
            
            try:
                common_paths = [
                    '.env', '.git/config', 'admin/', 'login/', 'backup.zip', 'config.php', 
                    'api/v1/users', 'server-status', 'robots.txt', 'wp-config.php', 
                    '.htaccess', 'db.sql', 'phpinfo.php', 'hidden/', 'test/'
                ]
                
                found_paths = []
                def check_dir(path):
                    url = f"{target_url}/{path}"
                    try:
                        r = requests.get(url, timeout=3, verify=False, allow_redirects=False)
                        if r.status_code in [200, 301, 302, 403]:
                            return f"[{r.status_code}] {url}"
                    except:
                        pass
                    return None

                with ThreadPoolExecutor(max_workers=20) as executor:
                    results = executor.map(check_dir, common_paths)
                    
                found_paths = [res for res in results if res]
                
                if found_paths:
                    output_text = f"DirBrute completed.\nFound {len(found_paths)} interesting paths:\n" + "\n".join(found_paths)
                else:
                    output_text = "DirBrute completed. No sensitive paths found from top 15 list."
                    
            except Exception as ex:
                output_text = f"Error during DirBrute: {ex}"
                
            result_message = f"DirBrute Output:\n```\n{output_text}\n```\n"
            yield f"\n```\n{output_text}\n```\n"
            
            summary_message = f"DirBrute Scan (`{target_url}`):\n{output_text}"
            
            messages.append({'role': 'assistant', 'content': ai_full_response})
            messages.append({'role': 'user', 'content': summary_message})
            
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT INTO messages (chat_id, sender, text, summary) VALUES (?, ?, ?, ?)", (chat_id, 'system', result_message, summary_message))
            conn.commit()
            conn.close()

        elif vuln_match and config.get('enable_autonomous_mode'):
            target_url = vuln_match.group(1).strip()
            if not target_url.startswith('http'):
                target_url = 'http://' + target_url
                
            yield f"\n\n*Autonomous Mode: Advanced Native VulnScan (SQLi, XSS, LFI, SSTI, SSRF, OS Cmd) on `{target_url}`...*\n"
            
            try:
                findings = []
                waf_detected = False
                scan_headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
                
                from urllib.parse import urlencode, urlparse, parse_qs, urlunparse
                
                def get_inject_urls(url, payload):
                    parsed = urlparse(url)
                    params = parse_qs(parsed.query)
                    if params:
                        injected = []
                        for key in params:
                            modified = dict(params)
                            modified[key] = [payload]
                            new_query = urlencode(modified, doseq=True)
                            injected.append(urlunparse(parsed._replace(query=new_query)))
                        return injected
                    return [url + "?vuln_test=" + payload]
                
                def is_waf(resp):
                    waf_sigs = ['cloudflare', 'incapsula', 'akamai', 'sucuri', 'access denied', 'request blocked', 'web application firewall']
                    return resp.status_code in [403, 406, 429] or any(s in resp.text.lower() for s in waf_sigs)
                
                # --- 1. SQL Injection ---
                sqli_payloads = ["'", "\"'", "1' OR '1'='1'--", "1 OR 1=1--", "'; WAITFOR DELAY '0:0:3'--"]
                sqli_errors = ["sql syntax", "mysql_fetch", "ora-", "sqlite3::", "unclosed quotation", "quoted string not properly terminated", "pg_query", "syntax error"]
                for p in sqli_payloads:
                    found = False
                    for test_url in get_inject_urls(target_url, p):
                        try:
                            r = requests.get(test_url, timeout=6, verify=False, headers=scan_headers)
                            if is_waf(r): waf_detected = True; continue
                            if any(err in r.text.lower() for err in sqli_errors):
                                findings.append(f"[CRITICAL] SQL Injection: {test_url}")
                                found = True; break
                        except: pass
                    if found: break
                        
                # --- 2. XSS Reflected ---
                xss_payloads = ["<script>alert('paxis')</script>", "\"><svg/onload=alert(1)>", "'><img src=x onerror=alert(1)>", "<body onload=alert(1)>"]
                for p in xss_payloads:
                    found = False
                    for test_url in get_inject_urls(target_url, p):
                        try:
                            r = requests.get(test_url, timeout=6, verify=False, headers=scan_headers)
                            if is_waf(r): waf_detected = True; continue
                            if p in r.text:
                                findings.append(f"[HIGH] Reflected XSS: {test_url}")
                                found = True; break
                        except: pass
                    if found: break
                        
                # --- 3. Local File Inclusion ---
                lfi_payloads = ["../../../../../../../../etc/passwd", "..%2F..%2F..%2F..%2Fetc%2Fpasswd", "....//....//etc/passwd", "..\\..\\..\\..\\windows\\win.ini"]
                for p in lfi_payloads:
                    found = False
                    for test_url in get_inject_urls(target_url, p):
                        try:
                            r = requests.get(test_url, timeout=6, verify=False, headers=scan_headers)
                            if "root:x:0:0" in r.text or "[extensions]" in r.text or "/bin/bash" in r.text:
                                findings.append(f"[CRITICAL] Local File Inclusion (LFI): {test_url}")
                                found = True; break
                        except: pass
                    if found: break

                # --- 4. SSTI (Server-Side Template Injection) ---
                ssti_map = {"{{7*7}}": "49", "${7*7}": "49", "#{7*7}": "49", "{{7*'7'}}": "7777777"}
                for p, expected in ssti_map.items():
                    found = False
                    for test_url in get_inject_urls(target_url, p):
                        try:
                            r = requests.get(test_url, timeout=6, verify=False, headers=scan_headers)
                            if expected in r.text:
                                findings.append(f"[CRITICAL] SSTI - Server-Side Template Injection: {test_url} | payload=`{p}`")
                                found = True; break
                        except: pass
                    if found: break

                # --- 5. OS Command Injection ---
                cmd_payloads = [";echo paxis_rce;", "|echo paxis_rce", "&&echo paxis_rce", "`echo paxis_rce`", "$(echo paxis_rce)"]
                for p in cmd_payloads:
                    found = False
                    for test_url in get_inject_urls(target_url, p):
                        try:
                            r = requests.get(test_url, timeout=6, verify=False, headers=scan_headers)
                            if "paxis_rce" in r.text:
                                findings.append(f"[CRITICAL] OS Command Injection: {test_url} | payload=`{p}`")
                                found = True; break
                        except: pass
                    if found: break

                # --- 6. SSRF ---
                ssrf_payloads = ["http://169.254.169.254/latest/meta-data/", "http://127.0.0.1:80", "http://[::1]:80"]
                for p in ssrf_payloads:
                    found = False
                    for test_url in get_inject_urls(target_url, p):
                        try:
                            r = requests.get(test_url, timeout=6, verify=False, headers=scan_headers)
                            if r.status_code == 200 and ("ami-id" in r.text or "instance-id" in r.text or "SSH-" in r.text):
                                findings.append(f"[CRITICAL] SSRF: {test_url} - internal service responded!")
                                found = True; break
                        except: pass
                    if found: break
                
                if waf_detected and not findings:
                    output_text = "VulnScan: WAF/Protection Detected (403/406/429 or WAF page). Recommend:\n  - <<EXPLOIT>> sqlmap --tamper=randomcase,space2comment,between --level=3 --risk=2 -u {url}\n  - Manually try URL-encoded payloads via <<EXECUTE>> curl"
                elif findings:
                    output_text = "Advanced VulnScan Completed! PAYABLE BUGS FOUND:\n" + "\n".join(findings)
                    if waf_detected:
                        output_text += "\n\n[NOTE] WAF also detected — these findings bypassed it!"
                else:
                    output_text = "Advanced VulnScan Completed. No SQLi, XSS, LFI, SSTI, OS Cmd Injection, or SSRF found at this endpoint."
                    
            except Exception as ex:
                output_text = f"Error during VulnScan: {ex}"
                
            result_message = f"VulnScan Output:\n```\n{output_text}\n```\n"
            yield f"\n```\n{output_text}\n```\n"
            
            summary_message = f"VulnScan (`{target_url}`):\n{output_text}"
            
            messages.append({'role': 'assistant', 'content': ai_full_response})
            messages.append({'role': 'user', 'content': summary_message})
            
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT INTO messages (chat_id, sender, text, summary) VALUES (?, ?, ?, ?)", (chat_id, 'system', result_message, summary_message))
            conn.commit()
            conn.close()

        elif subenum_match and config.get('enable_autonomous_mode'):
            target_domain = subenum_match.group(1).strip()
            yield f"\n\n*Autonomous Mode: Passive Subdomain Enumeration via crt.sh for `{target_domain}`...*\n"
            
            try:
                crtsh_url = f"https://crt.sh/?q=%.{target_domain}&output=json"
                r = requests.get(crtsh_url, timeout=20, headers={'User-Agent': 'Mozilla/5.0'})
                r.raise_for_status()
                data = r.json()
                
                subdomains = set()
                for entry in data:
                    name_value = entry.get('name_value', '')
                    for sub in name_value.split('\n'):
                        sub = sub.strip().lstrip('*.')
                        if sub and target_domain in sub:
                            subdomains.add(sub)
                
                sorted_subs = sorted(subdomains)
                output_text = f"Subdomain Enumeration for `{target_domain}` completed.\nFound {len(sorted_subs)} unique subdomains:\n" + "\n".join(sorted_subs[:100])
                if len(sorted_subs) > 100:
                    output_text += f"\n...and {len(sorted_subs)-100} more."
            except Exception as ex:
                output_text = f"Error during subdomain enumeration: {ex}"
                
            result_message = f"SubEnum Output:\n```\n{output_text}\n```\n"
            yield f"\n```\n{output_text}\n```\n"
            
            summary_message = f"Subdomain Enumeration (`{target_domain}`):\n{output_text}"
            
            messages.append({'role': 'assistant', 'content': ai_full_response})
            messages.append({'role': 'user', 'content': summary_message})
            
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT INTO messages (chat_id, sender, text, summary) VALUES (?, ?, ?, ?)", (chat_id, 'system', result_message, summary_message))
            conn.commit()
            conn.close()

        elif osint_match and config.get('enable_autonomous_mode'):
            osint_query = osint_match.group(1).strip()
            yield f"\n\n*Autonomous Mode: OSINT Fingerprinting for `{osint_query}`...*\n"
            
            try:
                import socket as _socket
                osint_results = []
                
                # DNS Resolution
                try:
                    ip = _socket.gethostbyname(osint_query)
                    osint_results.append(f"[+] DNS: {osint_query} -> {ip}")
                    try:
                        hostname = _socket.gethostbyaddr(ip)[0]
                        osint_results.append(f"[+] Reverse DNS: {ip} -> {hostname}")
                    except: pass
                except Exception as e:
                    osint_results.append(f"[-] DNS Resolution failed: {e}")
                
                # Check key web files
                for path in ['robots.txt', 'security.txt', '.well-known/security.txt', 'sitemap.xml', 'crossdomain.xml']:
                    for scheme in ['https', 'http']:
                        try:
                            url = f"{scheme}://{osint_query}/{path}"
                            r = requests.get(url, timeout=5, verify=False, headers={'User-Agent': 'Mozilla/5.0'})
                            if r.status_code == 200:
                                osint_results.append(f"[+] FOUND {url}:\n{r.text[:500]}")
                                break
                        except: pass

                # Technology Fingerprinting via headers
                try:
                    r = requests.get(f"https://{osint_query}", timeout=8, verify=False, headers={'User-Agent': 'Mozilla/5.0'})
                    tech_hdrs = ['server', 'x-powered-by', 'x-generator', 'x-aspnet-version', 'x-frame-options', 'content-security-policy', 'strict-transport-security', 'x-amz-request-id']
                    for h in tech_hdrs:
                        if h in r.headers:
                            osint_results.append(f"[+] Header [{h}]: {r.headers[h]}")
                except Exception as e:
                    osint_results.append(f"[-] HTTP Header fetch failed: {e}")
                
                output_text = f"OSINT Fingerprinting for `{osint_query}`:\n" + "\n".join(osint_results)
            except Exception as ex:
                output_text = f"Error during OSINT: {ex}"
                
            result_message = f"OSINT Output:\n```\n{output_text}\n```\n"
            yield f"\n```\n{output_text}\n```\n"
            
            summary_message = f"OSINT (`{osint_query}`):\n{output_text}"
            
            messages.append({'role': 'assistant', 'content': ai_full_response})
            messages.append({'role': 'user', 'content': summary_message})
            
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT INTO messages (chat_id, sender, text, summary) VALUES (?, ?, ?, ?)", (chat_id, 'system', result_message, summary_message))
            conn.commit()
            conn.close()

        elif mcp_match and mcp_client:
            tool_name = mcp_match.group(1).strip()
            args_str = mcp_match.group(2).strip()
            
            yield f"\n\n*MCP Agent: Executing tool {tool_name}...*\n"
            
            try:
                # Try to parse args as JSON, if loose format, might fail
                try:
                    tool_args = json.loads(args_str)
                except:
                    # Fallback or pass as string if tool expects it? 
                    # For now assuming LLM produces valid JSON or simple args
                    tool_args = {"arg": args_str} 
                
                result = mcp_client.execute_tool(tool_name, tool_args)
            except Exception as ex:
                result = f"Error executing tool: {ex}"

            result_message = f"Tool Output:\n```\n{result}\n```\n"
            yield f"\n```\n{result}\n```\n"
            
            summary_text = summarize_tool_output(model_name, f"MCP:{tool_name}", str(result))
            summary_message = f"Tool Output Summary (`{tool_name}`):\n{summary_text}\n"
             
            messages.append({'role': 'assistant', 'content': ai_full_response})
            messages.append({'role': 'user', 'content': summary_message})
            
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT INTO messages (chat_id, sender, text, summary) VALUES (?, ?, ?, ?)", (chat_id, 'system', result_message, summary_message))
            conn.commit()
            conn.close()

        else:
            if config.get('enable_autonomous_mode'):
                if "# Final Report" in ai_full_response:
                    yield "\n\n*Agent finished its task based on Final Report indicator.*\n"
                    break
                else:
                    warning = "System: You did not execute a command using the <<EXECUTE>> command <<END>> format or output '# Final Report'. Please provide your next command or final report."
                    yield f"\n\n*{warning}*\n"
                    messages.append({'role': 'assistant', 'content': ai_full_response})
                    messages.append({'role': 'user', 'content': warning})
                    
                    conn = sqlite3.connect(DB_NAME)
                    c = conn.cursor()
                    c.execute("INSERT INTO messages (chat_id, sender, text, summary) VALUES (?, ?, ?, ?)", (chat_id, 'system', warning, warning))
                    conn.commit()
                    conn.close()
                    continue
            else:
                break
@paxis.route('/upload_file', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"success": False, "message": "No file part"}), 400
    
    file = request.files['file']
    chat_id = request.form.get('chat_id')
    user_hash = request.cookies.get('user_hash')

    if not user_hash:
        return jsonify({"success": False, "message": "User hash not found."}), 401

    if file.filename == '':
        return jsonify({"success": False, "message": "No selected file"}), 400
    
    if not chat_id:
        return jsonify({"success": False, "message": "No chat ID"}), 400

    if file:
        filename = secure_filename(file.filename)
        chat_upload_dir = os.path.join(paxis.config['UPLOAD_FOLDER'], chat_id)
        os.makedirs(chat_upload_dir, exist_ok=True)
        
        file_path = os.path.join(chat_upload_dir, filename)
        file.save(file_path)
        
        web_path = f"/uploads/{chat_id}/{filename}"
        return jsonify({"success": True, "file_path": web_path, "file_name": filename})

@paxis.route('/uploads/<chat_id>/<path:filename>')
def uploaded_file(chat_id, filename):
    chat_upload_dir = os.path.join(paxis.config['UPLOAD_FOLDER'], chat_id)
    return send_from_directory(chat_upload_dir, filename)


@paxis.route('/execute_stream', methods=['POST'])
def execute_stream():
    command = request.json.get("command")
    chat_id = request.json.get("chat_id")
    output_id = request.json.get("output_id")
    user_hash = request.cookies.get('user_hash')

    if not user_hash:
        return jsonify({"success": False, "message": "User hash not found."}), 401

    if not all([command, chat_id, output_id]):
        return jsonify({"success": False, "message": "Missing required data."}), 400

    full_output = ""
    
    def generate_and_save():
        nonlocal full_output
        try:
            # Enabled shell=True for full command execution capability
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            
            for line in iter(process.stdout.readline, ''):
                full_output += line
                yield line
                
            process.stdout.close()
            return_code = process.wait()
            final_status = f"\n[Process finished with exit code {return_code}]\n" if return_code != 0 else f"\n[Process finished successfully]\n"
            full_output += final_status
            yield final_status

        except FileNotFoundError:
            full_output = "[Error: Command not found. Please check your command and environment path.]"
            yield full_output
        except Exception as e:
            full_output = f"[Error: {str(e)}]"
            yield full_output
        finally:
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("INSERT OR REPLACE INTO command_outputs (output_id, chat_id, command, output) VALUES (?, ?, ?, ?)",
                      (output_id, chat_id, command, full_output))
            conn.commit()
            conn.close()

    return Response(stream_with_context(generate_and_save()), mimetype="text/plain")

@paxis.route('/get_command_output', methods=['POST'])
def get_command_output():
    output_id = request.json.get("output_id")
    if not output_id:
        return jsonify({"success": False, "message": "Output ID not provided."}), 400

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT command, output FROM command_outputs WHERE output_id = ?", (output_id,))
    result = c.fetchone()
    conn.close()

    if result:
        return jsonify({"success": True, "command": result[0], "output": result[1]})
    else:
        return jsonify({"success": False, "message": "Output not found."}), 404

@paxis.route('/')
def index():
    return render_template('index.html', page_mode='chats', active_project_id=None, active_project_title=None)

@paxis.route('/projects')
def projects_page():
    return render_template('index.html', page_mode='projects', active_project_id=None, active_project_title=None)

@paxis.route('/project/<project_id>')
def project_detail_page(project_id):
    user_hash = request.cookies.get('user_hash')
    project_title = "Project" 
    
    if user_hash:
        try:
            conn = sqlite3.connect(DB_NAME)
            c = conn.cursor()
            c.execute("SELECT title FROM projects WHERE project_id = ? AND user_hash = ?", (project_id, user_hash))
            project = c.fetchone()
            conn.close()
            if project:
                project_title = project[0]
            else:
                project_title = "Unknown Project"
        except Exception as e:
            logger.error(f"Error fetching project title: {e}")
            project_title = "Error"

    return render_template('index.html', page_mode='project_detail', active_project_id=project_id, active_project_title=project_title)

@paxis.route('/login', methods=['POST'])
def login():
    username = request.json.get("username")
    if not username:
        return jsonify({"success": False, "message": "Username not provided."}), 400
    
    user_hash = hashlib.sha256(secrets.token_bytes(32)).hexdigest()

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT user_hash FROM users WHERE username = ?", (username,))
    existing_user = c.fetchone()
    if existing_user:
        user_hash = existing_user[0]
    else:
        c.execute("INSERT INTO users (user_hash, username) VALUES (?, ?)", (user_hash, username))
        conn.commit()
    conn.close()

    response = make_response(jsonify({"success": True, "user_hash": user_hash, "username": username}))
    response.set_cookie('user_hash', user_hash, max_age=60*60*24*365) 
    return response

@paxis.route('/get_user_info', methods=['GET'])
def get_user_info():
    user_hash = request.cookies.get('user_hash')
    if not user_hash:
        return jsonify({"success": False, "message": "User hash not found."}), 401

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT username FROM users WHERE user_hash = ?", (user_hash,))
    user_info = c.fetchone()
    conn.close()

    if user_info:
        return jsonify({"success": True, "username": user_info[0]})
    else:
        return jsonify({"success": False, "message": "User not found."}), 404


@paxis.route('/get_chats', methods=['GET'])
def get_chats():
    user_hash = request.cookies.get('user_hash')
    if not user_hash:
        return jsonify({"success": False, "message": "User hash not found."}), 401
    
    project_id = request.args.get('project_id')

    if not project_id or project_id == 'null' or project_id == 'None':
        project_id = None

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    if project_id: 
        c.execute(
            "SELECT chat_id, title, model_name FROM chats WHERE user_hash = ? AND project_id = ? ORDER BY timestamp DESC", 
            (user_hash, project_id)
        )
    else:
         c.execute(
            "SELECT chat_id, title, model_name FROM chats WHERE user_hash = ? AND (project_id IS NULL OR project_id = 'None') ORDER BY timestamp DESC", 
            (user_hash,)
        )
        
    chat_list = [{"chat_id": row[0], "title": row[1], "model_name": row[2]} for row in c.fetchall()]
    conn.close()
    return jsonify({"success": True, "chats": chat_list})

@paxis.route('/get_chat_messages', methods=['POST'])
def get_chat_messages():
    chat_id = request.json.get("chat_id")
    if not chat_id:
        return jsonify({"success": False, "message": "Chat ID not provided."}), 400

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT sender, text, file_path, file_name FROM messages WHERE chat_id = ? ORDER BY timestamp ASC", (chat_id,))
    messages = [{"sender": row[0], "text": row[1], "file_path": row[2], "file_name": row[3]} for row in c.fetchall()]
    conn.close()
    return jsonify({"success": True, "messages": messages})

@paxis.route('/rename_chat', methods=['POST'])
def rename_chat():
    chat_id = request.json.get("chat_id")
    new_title = request.json.get("new_title")
    user_hash = request.cookies.get('user_hash')

    if not all([chat_id, new_title, user_hash]):
        return jsonify({"success": False, "message": "Missing required data."}), 400
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE chats SET title = ? WHERE chat_id = ? AND user_hash = ?", (new_title, chat_id, user_hash))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@paxis.route('/delete_chat', methods=['POST'])
def delete_chat():
    chat_id = request.json.get("chat_id")
    user_hash = request.cookies.get('user_hash')
    
    if not all([chat_id, user_hash]):
        return jsonify({"success": False, "message": "Missing required data."}), 400

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM chats WHERE chat_id = ? AND user_hash = ?", (chat_id, user_hash))
    conn.commit()
    conn.close()
    return jsonify({"success": True})
    
@paxis.route('/create_new_chat', methods=['POST'])
def create_new_chat():
    user_hash = request.cookies.get('user_hash')
    model_name = request.json.get("model_name")
    project_id = request.json.get("project_id") 

    if not user_hash or not model_name:
        return jsonify({"success": False, "message": "Missing user hash or model name."}), 400

    if not project_id or project_id == 'null' or project_id == 'None':
        project_id = None

    chat_id = str(uuid.uuid4())
    default_title = "New Chat"
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        "INSERT INTO chats (chat_id, user_hash, title, model_name, project_id) VALUES (?, ?, ?, ?, ?)", 
        (chat_id, user_hash, default_title, model_name, project_id)
    )
    conn.commit()
    conn.close()

    return jsonify({"success": True, "chat_id": chat_id, "title": default_title, "model_name": model_name})

@paxis.route('/chat_stream', methods=['POST'])
def chat_stream():
    user_message = request.json.get("message")
    chat_id = request.json.get("chat_id")
    model_name = request.json.get("model_name")
    user_hash = request.cookies.get('user_hash')
    file_path = request.json.get("file_path")
    file_name = request.json.get("file_name")

    if not all([user_message, chat_id, model_name, user_hash]):
        return jsonify({"response": "Missing chat data."}), 400

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    c.execute("SELECT * FROM messages WHERE chat_id = ?", (chat_id,))
    is_first_message = not c.fetchone()
    if is_first_message:
        chat_title = user_message[:25] + "..." if len(user_message) > 25 else user_message
        c.execute("UPDATE chats SET title = ? WHERE chat_id = ?", (chat_title, chat_id))
        conn.commit()

    c.execute("INSERT INTO messages (chat_id, sender, text, summary, file_path, file_name) VALUES (?, ?, ?, ?, ?, ?)", 
              (chat_id, 'user', user_message, user_message, file_path, file_name))
    conn.commit()
    conn.close()

    history = get_chat_history_for_ollama(chat_id)
    config = load_config()
    
    return Response(stream_with_context(stream_ollama_response(model_name, history, user_message, chat_id, config)),
                    mimetype="text/plain")

@paxis.route('/get_models', methods=['GET'])
def get_models():
    ollama_url = "http://localhost:11434/api/tags"
    try:
        r = requests.get(ollama_url)
        r.raise_for_status()
        models_data = r.json()
        models = []
        for model in models_data.get('models', []):
            model_name = model['name']
            models.append(model_name)
        return jsonify({"success": True, "models": models})
    except requests.exceptions.RequestException as e:
        logger.error(f"[Ollama Get Models Error] {e}")
        return jsonify({"success": False, "message": f"Error fetching models: {e}"}), 500

@paxis.route('/get_projects', methods=['GET'])
def get_projects():
    user_hash = request.cookies.get('user_hash')
    if not user_hash:
        return jsonify({"success": False, "message": "User hash not found."}), 401
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT project_id, title FROM projects WHERE user_hash = ? ORDER BY timestamp DESC", (user_hash,))
    project_list = [{"project_id": row[0], "title": row[1]} for row in c.fetchall()]
    conn.close()
    return jsonify({"success": True, "projects": project_list})

@paxis.route('/create_new_project', methods=['POST'])
def create_new_project():
    user_hash = request.cookies.get('user_hash')
    project_name = request.json.get("project_name")

    if not user_hash or not project_name:
        return jsonify({"success": False, "message": "Missing user hash or project name."}), 400

    project_id = str(uuid.uuid4())
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        "INSERT INTO projects (project_id, user_hash, title) VALUES (?, ?, ?)", 
        (project_id, user_hash, project_name)
    )
    conn.commit()
    conn.close()

    return jsonify({"success": True, "project_id": project_id, "title": project_name})

@paxis.route('/rename_project', methods=['POST'])
def rename_project():
    project_id = request.json.get("project_id")
    new_title = request.json.get("new_title")
    user_hash = request.cookies.get('user_hash')

    if not all([project_id, new_title, user_hash]):
        return jsonify({"success": False, "message": "Missing required data."}), 400
    
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE projects SET title = ? WHERE project_id = ? AND user_hash = ?", (new_title, project_id, user_hash))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@paxis.route('/delete_project', methods=['POST'])
def delete_project():
    project_id = request.json.get("project_id")
    user_hash = request.cookies.get('user_hash')
    
    if not all([project_id, user_hash]):
        return jsonify({"success": False, "message": "Missing required data."}), 400

    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM projects WHERE project_id = ? AND user_hash = ?", (project_id, user_hash))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@paxis.route('/settings')
def settings_page():
    return render_template('index.html', page_mode='settings', active_project_id=None, active_project_title=None)

@paxis.route('/api/get_settings', methods=['GET'])
def get_settings():
    return jsonify(load_config())

@paxis.route('/api/save_settings', methods=['POST'])
def update_settings():
    new_config = request.json
    if save_config(new_config):
        return jsonify({"success": True, "message": "Settings saved successfully."})
    else:
        return jsonify({"success": False, "message": "Failed to save settings."}), 500

if __name__ == '__main__':
    try:
        init_db()
    except sqlite3.OperationalError:
        print("Database already initialized.")
    
    print("Paxis server is running on ::: http://127.0.0.1:5000")
    serve(paxis, host='127.0.0.1', port=5000)
