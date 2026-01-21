import asyncio
import json
import sys
import os
from dotenv import load_dotenv
import re
import requests
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from openai import OpenAI
from email_validator import validate_email, EmailNotValidError

import time


# Carregar variáveis do .env
load_dotenv()
API_KEY = os.getenv("OPENAI_API_KEY")
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY")

client = OpenAI(api_key=API_KEY)


def load_items(input_path: str):
    """Carrega itens do arquivo de entrada.

    Suporta:
      - .json  : lista JSON tradicional
      - .jsonl : JSON Lines (1 objeto por linha)
    """
    _, ext = os.path.splitext(input_path.lower())
    if ext == ".jsonl":
        items = []
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    items.append(json.loads(line))
                except Exception:
                    # se uma linha estiver quebrada, ignora para não parar o job inteiro
                    continue
        return items

    with open(input_path, "r", encoding="utf-8") as f:
        return json.load(f)


def iter_jsonl_lines_follow(path: str, poll_interval: float = 0.5):
    """Itera linhas JSONL à medida que elas são adicionadas ao arquivo.

    - Funciona estilo "tail -f".
    - Retorna dicts (json.loads por linha).
    - Para quando encontra uma linha especial: "__END__".
    """
    with open(path, "r", encoding="utf-8") as f:
        while True:
            line = f.readline()
            if not line:
                time.sleep(poll_interval)
                continue
            line = line.strip()
            if not line:
                continue
            if line == "__END__":
                return
            try:
                yield json.loads(line)
            except Exception:
                # Linha parcial / corrompida: ignora
                continue


def iter_jsonl_objects(path: str):
    """Itera objetos de um arquivo JSONL já finalizado (um JSON por linha).

    Ignora linhas vazias, corrompidas e o marcador __END__.
    """
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line == "__END__":
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

# Load Company Profile for Personalized Messages
COMPANY_PROFILE = {}
try:
    profile_path = os.path.join(os.path.dirname(__file__), 'company_profile.json')
    if os.path.exists(profile_path):
        with open(profile_path, 'r', encoding='utf-8') as f:
            COMPANY_PROFILE = json.load(f)
        print(f"📋 Loaded company profile: {COMPANY_PROFILE.get('company_name', 'Unknown')}")
except Exception as e:
    print(f"⚠️ Could not load company_profile.json: {e}")

def extract_tax_id(text, country_code=None):
    """
    Extracts tax ID from text based on country.
    Supports: CNPJ (BR), EIN (US), VAT (EU), ABN (AU), RFC (MX), CUIT (AR), NIF (PT), Company Number (GB)
    """
    if not text:
        return None
    
    # Country-specific patterns
    patterns = {
        'BR': {
            'pattern': r'\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}',
            'type': 'CNPJ',
            'name': 'Cadastro Nacional da Pessoa Jurídica'
        },
        'US': {
            'pattern': r'\d{2}-\d{7}',
            'type': 'EIN',
            'name': 'Employer Identification Number'
        },
        'GB': {
            'pattern': r'(?<![0-9])\d{8}(?![0-9])',
            'type': 'Company Number',
            'name': 'UK Company Registration Number'
        },
        'AU': {
            'pattern': r'\d{2}\s?\d{3}\s?\d{3}\s?\d{3}',
            'type': 'ABN',
            'name': 'Australian Business Number'
        },
        'MX': {
            'pattern': r'[A-Z]{3,4}\d{6}[A-Z0-9]{3}',
            'type': 'RFC',
            'name': 'Registro Federal de Contribuyentes'
        },
        'AR': {
            'pattern': r'\d{2}-\d{8}-\d',
            'type': 'CUIT',
            'name': 'Clave Única de Identificación Tributaria'
        },
        'PT': {
            'pattern': r'(?<![0-9])\d{9}(?![0-9])',
            'type': 'NIF',
            'name': 'Número de Identificação Fiscal'
        },
        'DE': {
            'pattern': r'DE\d{9}',
            'type': 'VAT',
            'name': 'Umsatzsteuer-Identifikationsnummer'
        },
        'FR': {
            'pattern': r'FR\d{11}',
            'type': 'VAT',
            'name': 'Numéro de TVA intracommunautaire'
        },
        'ES': {
            'pattern': r'ES[A-Z0-9]\d{7}[A-Z0-9]',
            'type': 'NIF/CIF',
            'name': 'Número de Identificación Fiscal'
        },
        'IT': {
            'pattern': r'IT\d{11}',
            'type': 'VAT',
            'name': 'Partita IVA'
        },
        'CA': {
            'pattern': r'\d{9}[A-Z]{2}\d{4}',
            'type': 'BN',
            'name': 'Business Number'
        }
    }
    
    # If country specified, try that pattern first
    if country_code and country_code.upper() in patterns:
        pattern_info = patterns[country_code.upper()]
        match = re.search(pattern_info['pattern'], text, re.IGNORECASE)
        if match:
            return {
                'type': pattern_info['type'],
                'value': match.group(0),
                'country': country_code.upper(),
                'name': pattern_info['name']
            }
    
    # If no country or not found, try Brazil first (most common in our use case)
    if not country_code or country_code.upper() == 'BR':
        br_match = re.search(patterns['BR']['pattern'], text)
        if br_match:
            return {
                'type': 'CNPJ',
                'value': br_match.group(0),
                'country': 'BR',
                'name': 'Cadastro Nacional da Pessoa Jurídica'
            }
    
    # Try EU VAT patterns
    eu_vat_pattern = r'[A-Z]{2}\d{9,12}'
    vat_match = re.search(eu_vat_pattern, text.upper())
    if vat_match:
        vat_value = vat_match.group(0)
        return {
            'type': 'VAT',
            'value': vat_value,
            'country': vat_value[:2],
            'name': 'VAT Number'
        }
    
    return None

def fetch_cnpja_data(cnpj):
    """
    Fetch corporate data from CNPJA open API.
    Returns structured company data including shareholders, equity, status, etc.
    """
    if not cnpj:
        return None
    
    # Extract digits only
    digits = "".join(filter(str.isdigit, cnpj))
    if len(digits) != 14:
        print(f"      ⚠️ Invalid CNPJ format: {cnpj}")
        return None
    
    url = f"https://open.cnpja.com/office/{digits}"
    print(f"      🏢 Fetching CNPJA data for: {cnpj}...")
    
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json()
            
            # Structure the relevant data
            cnpja_result = {
                "company_name": data.get("company", {}).get("name"),
                "alias": data.get("alias"),
                "status": data.get("status", {}).get("text"),
                "status_date": data.get("statusDate"),
                "founded": data.get("founded"),
                "equity": data.get("company", {}).get("equity"),
                "nature": data.get("company", {}).get("nature", {}).get("text"),
                "size": data.get("company", {}).get("size", {}).get("text"),
                "main_activity": data.get("mainActivity", {}).get("text"),
                "side_activities": [a.get("text") for a in data.get("sideActivities", [])],
                "shareholders": [],
                "official_address": None,
                "official_phones": [],
                "official_emails": []
            }
            
            # Extract shareholders
            for member in data.get("company", {}).get("members", []):
                person = member.get("person", {})
                cnpja_result["shareholders"].append({
                    "name": person.get("name"),
                    "role": member.get("role", {}).get("text"),
                    "since": member.get("since"),
                    "age_range": person.get("age")
                })
            
            # Extract official address
            addr = data.get("address", {})
            if addr:
                cnpja_result["official_address"] = {
                    "street": addr.get("street"),
                    "number": addr.get("number"),
                    "district": addr.get("district"),
                    "city": addr.get("city"),
                    "state": addr.get("state"),
                    "zip": addr.get("zip")
                }
            
            # Extract phones (format to E.164)
            for phone in data.get("phones", []):
                area = phone.get("area", "")
                number = phone.get("number", "")
                cnpja_result["official_phones"].append(f"+55{area}{number}")
            
            # Extract emails
            for email in data.get("emails", []):
                cnpja_result["official_emails"].append(email.get("address"))
            
            print(f"      ✅ CNPJA: {cnpja_result['company_name']} ({cnpja_result['status']})")
            return cnpja_result
        else:
            print(f"      ⚠️ CNPJA API returned status: {response.status_code}")
            return None
    except Exception as e:
        print(f"      ⚠️ CNPJA API error: {e}")
        return None

def detect_tech_stack(html_content):
    """
    Detects common technologies in HTML content.
    """
    if not html_content: return []
    
    stack = []
    lower_html = html_content.lower()
    
    checks = {
        "WordPress": ["wp-content", "generator\" content=\"wordpress"],
        "Wix": ["wix.com", "wix-dns"],
        "RD Station": ["d335luupugsy2.cloudfront.net", "rdstation"],
        "Shopify": ["shopify"],
        "Google Analytics": ["googletagmanager", "ua-", "g-"],
        "Facebook Pixel": ["fbevents.js"],
        "Hotjar": ["hotjar"],
        "Vercel": ["vercel"],
        "Next.js": ["__next"],
        "Nuxt.js": ["__nuxt"]
    }
    
    for tech, keywords in checks.items():
        if any(k in lower_html for k in keywords):
            stack.append(tech)
            
    return stack

def verify_email_address(email):
    try:
        v = validate_email(email, check_deliverability=True)
        return v.normalized
    except EmailNotValidError:
        return None

def detect_whatsapp(phone):
    """
    Heuristic to detect Brazilian mobile numbers and generate WhatsApp link.
    Assumes phone is a string like "(11) 99999-9999" or "+55 11 ..."
    """
    if not phone: return None
    
    # Remove non-digits
    digits = "".join(filter(str.isdigit, phone))
    
    # Check for Brazil DDI (55) or imply it if length is 10-11
    if len(digits) == 11: # DDD + 9 digits (Mobile)
        # 3rd digit should be 9 for mobile in most cases, but let's be broad
        return f"https://wa.me/55{digits}"
    elif len(digits) == 10: # DDD + 8 digits (Old mobile or Landline)
        # Harder to distinguish, but if it starts with 55...
        pass
    elif len(digits) >= 12 and digits.startswith("55"):
        # Full number
        if len(digits) == 13: # 55 + 11 digits
             return f"https://wa.me/{digits}"
             
    return None

def fetch_hunter_emails(domain):
    BLACKLIST_DOMAINS = ['instagram.com', 'facebook.com', 'linkedin.com', 'linktr.ee', 'twitter.com', 'youtube.com', 'tiktok.com']
    
    if any(b in domain for b in BLACKLIST_DOMAINS):
        print(f"      🚫 Skipping Hunter for social domain: {domain}")
        return []

    print(f"      🔫 Hunting emails for: {domain}...")
    url = f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={HUNTER_API_KEY}"
    try:
        # Hunter tem rate limit; quando dá 429 significa "muitas requisições".
        # Vamos tentar poucas vezes com backoff e respeitando Retry-After, quando presente.
        attempts = 0
        max_attempts = 3
        while attempts < max_attempts:
            attempts += 1
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                raw_emails = data.get('data', {}).get('emails', [])
                valid_emails = []
                for e in raw_emails:
                    email_val = e.get('value')
                    if email_val:
                        verified = verify_email_address(email_val)
                        if verified:
                            valid_emails.append(verified)

                print(f"      🎯 Hunter found {len(valid_emails)} VALID emails (from {len(raw_emails)} raw)!")
                return valid_emails

            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                try:
                    wait_seconds = int(retry_after) if retry_after else (2 ** attempts)
                except Exception:
                    wait_seconds = (2 ** attempts)
                print(f"      ⚠️ Hunter rate limit (429). Waiting {wait_seconds}s then retrying ({attempts}/{max_attempts})...")
                time.sleep(wait_seconds)
                continue

            # Qualquer outro status: não vale insistir muito.
            print(f"      ⚠️ Hunter API Error: {response.status_code}")
            return []

        print("      ⚠️ Hunter rate limit persisted after retries. Skipping Hunter for this lead.")
        return []
    except Exception as e:
        print(f"      ⚠️ Hunter Exception: {e}")
        return []

async def analyze_with_gpt(content, raw_lead_data, hunter_data=[], output_lang='en', search_country=None):
    company_name = raw_lead_data.get('nome_empresa', 'Unknown')
    print(f"      🤖 Analyzing and Consolidating data for {company_name}...")
    try:
        hunter_context = ""
        if hunter_data:
            hunter_context = f"\nVERIFIED EMAILS FROM HUNTER.IO:\n{json.dumps(hunter_data, indent=2)}\n"

        # Build language instruction
        lang_names = {
            'pt': 'Portuguese', 'en': 'English', 'es': 'Spanish', 'fr': 'French',
            'de': 'German', 'it': 'Italian', 'ja': 'Japanese', 'zh': 'Chinese',
            'ko': 'Korean', 'ar': 'Arabic', 'ru': 'Russian'
        }
        lang_instruction = f"OUTPUT ALL TEXT FIELDS IN {lang_names.get(output_lang, 'English').upper()}."
        
        country_instruction = ""
        if search_country:
            country_instruction = f"The business is located in {search_country}. Include country and country_code (ISO 3166-1 alpha-2) in address_components."

        prompt = f"""
        You are a Data Cleaning and Business Intelligence Expert.
        
        IMPORTANT: {lang_instruction}
        {country_instruction}
        
        I have raw data from Google Maps, unstructured text scraped from the company's website, and VERIFIED EMAILS from Hunter.io.
        Your goal is to MERGE, VALIDATE, and CLEAN this information into a single, perfect JSON profile.
        
        RAW GOOGLE MAPS DATA:
        {json.dumps(raw_lead_data, ensure_ascii=False)}
        
        {hunter_context}
        
        SCRAPED WEBSITE CONTENT (Markdown):
        {content[:20000]}
        
        OUR COMPANY PROFILE (For generating personalized outreach messages):
        {json.dumps(COMPANY_PROFILE, ensure_ascii=False, indent=2) if COMPANY_PROFILE else 'Not available'}
        
        INSTRUCTIONS:
        1. Verify the company name and address. PARSE THE ADDRESS into components (street, number, neighborhood, city, state, zip, country, country_code).
        2. Merge phone numbers. STANDARDIZE them to E.164 format (e.g., +5511999999999).
        3. Extract a professional business summary.
        4. List key products/services.
        5. Identify the target audience.
        6. Extract specific valuable contacts (Names + Roles).
        7. Determine the business category/sector.
        8. EXTRACT SOCIAL MEDIA INSIGHTS.
        9. MERGE EMAILS: Prioritize "Verified Emails" from Hunter.io. If none, look for emails in the website text.
        10. PRESERVE MAPS METADATA: Copy 'is_claimed', 'plus_code', 'available_actions', 'google_attributes', 'avaliacao', 'numero_avaliacoes', 'horario_funcionamento', 'latitude', 'longitude' etc. from the RAW GOOGLE MAPS DATA into 'google_maps_metadata'.
        11. GENERATE PERSONALIZED OUTREACH MESSAGES:
            - Analyze the lead's business type, services, and potential pain points.
            - Using OUR COMPANY PROFILE, create:
                a) EMAIL: Professional email with personalized subject and body (3-5 sentences). Reference specific aspects of the lead's business that our services can help with.
                b) WHATSAPP: Short, informal message (2-3 sentences) with emoji. Direct value proposition for this specific lead.
            - Messages must be in the OUTPUT LANGUAGE specified above.
            - If a contact name is available from key_people, use it in the greeting.
        
        OUTPUT JSON FORMAT:
        {{
            "company_info": {{
                "name": "Verified Name",
                "tax_id": {{
                    "type": "CNPJ or EIN or VAT or ABN or RFC or CUIT or null",
                    "value": "12.345.678/0001-90 or 12-3456789 or null",
                    "country": "BR or US or DE or null"
                }},
                "description": "Professional Summary",
                "category": "Verified Category",
                "sentiment": "Professional Assessment"
            }},
            "contact_details": {{
                "address": "Full Formatted Address",
                "address_components": {{
                    "street": "Rua X",
                    "number": "123",
                    "neighborhood": "Bairro",
                    "city": "Cidade",
                    "state": "SP",
                    "zip_code": "00000-000",
                    "country": "Brazil",
                    "country_code": "BR"
                }},
                "phones": ["+5511999999999"],
                "emails": ["email1@domain.com", "email2@domain.com"],
                "website": "URL",
                "social_media": {{
                    "instagram": "url or null",
                    "facebook": "url or null",
                    "linkedin": "url or null"
                }}
            }},
            "business_intelligence": {{
                "tech_stack": ["WordPress", "Google Analytics"],
                "products_services": ["Product A", "Service B"],
                "target_audience": "Description of target audience",
                "key_people": [
                    {{"name": "Person Name", "role": "Role"}}
                ],
                "social_media_insights": {{
                    "bio": "Extracted Bio",
                    "followers": "Follower count",
                    "latest_activity": "Latest post"
                }},
                "hunter_io_verified": true
            }},
                "google_maps_metadata": {{
                "is_claimed": true/false,
                "plus_code": "Code",
                "located_in": "Location",
                "google_description": "Description from Maps",
                "opening_hours": "Mon-Fri 9am-6pm",
                "coordinates": {{
                    "latitude": "-23.5505",
                    "longitude": "-46.6333"
                }},
                "rating": "4.8",
                "reviews": "150",
                "available_actions": ["Reservar", "Menu"],
                "google_attributes": {{
                    "raw_about_text": "Full text from About tab"
                }}
            }},
            "outreach_messages": {{
                "email": {{
                    "subject": "Personalized subject line based on lead's business",
                    "body": "Professional email body (3-5 sentences) explaining how our services can help THIS SPECIFIC lead. Include greeting with contact name if available."
                }},
                "whatsapp": "Short WhatsApp message (2-3 sentences) with emoji. Informal but professional. Direct value proposition for THIS lead."
            }}
        }}
        """
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that outputs ONLY valid JSON."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )
        
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"      ⚠️ AI Analysis failed: {e}")
        return None

async def crawl_page(crawler, url, context_name="Home"):
    print(f"   🕷️ Crawling {context_name}: {url}...")
    try:
        config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            word_count_threshold=10,
            excluded_tags=['nav', 'footer', 'header', 'script', 'style'],
        )
        result = await crawler.arun(url=url, config=config)
        if result.success:
            print(f"      ✅ {context_name} crawled ({len(result.markdown)} chars)")
            return result
        else:
            print(f"      ❌ Failed to crawl {context_name}: {result.error_message}")
            return None
    except Exception as e:
        print(f"      ⚠️ Exception crawling {context_name}: {e}")
        return None

async def main():
    if len(sys.argv) < 2:
        print("Usage: python enrich_leads.py <json_file> [lang] [country]")
        return

    json_file = sys.argv[1]
    output_lang = sys.argv[2] if len(sys.argv) > 2 else 'en'
    search_country = sys.argv[3] if len(sys.argv) > 3 else None
    
    if not os.path.exists(json_file):
        print(f"File not found: {json_file}")
        return

    stream_mode = json_file.lower().endswith('.jsonl')

    print(f"Loading data from {json_file}...")
    print(f"🌐 Output Language: {output_lang}")
    if search_country:
        print(f"🏳️ Search Country: {search_country}")

    # No modo stream, não carregamos tudo de uma vez; vamos consumindo linha a linha
    items = [] if stream_mode else load_items(json_file)

    # Configure Browser (Anti-blocking / Stealth)
    browser_config = BrowserConfig(
        browser_type="chromium",
        headless=False, # Visual Mode Enabled
        verbose=True,
    )

    final_leads = []

    # Sempre salvar na pasta do script (independente do current working directory)
    out_dir = os.path.dirname(os.path.abspath(__file__))
    final_file = os.path.join(out_dir, "final_leads.json")

    print(f"📁 Output folder: {out_dir}")
    print(f"📄 Final output (array): {final_file}")

    print("Starting Crawl4AI (Deep Mode)...")
    async with AsyncWebCrawler(config=browser_config) as crawler:

        if stream_mode:
            processed = 0
            print("🧵 Stream mode ON (processando leads conforme chegam no arquivo .jsonl).")

            # 1) Processa o que já existe no arquivo (caso o Node tenha escrito antes do worker estar pronto)
            for item in iter_jsonl_objects(json_file):
                processed += 1
                i = processed - 1
                # ---- processamento do lead (mesma lógica do loop normal) ----
                base_url = item.get('website')
                hunter_emails = []
                has_website = False
                if base_url and "google.com" not in base_url and item.get('website') != "Não disponível":
                    has_website = True
                    if not base_url.startswith('http'):
                        base_url = 'http://' + base_url
                    print(f"[{processed}] Processing {item.get('nome_empresa')} ({base_url})...")
                else:
                    print(f"[{processed}] Processing {item.get('nome_empresa')} (No Website - Maps Data Only)...")

                full_content = ""

                if has_website:
                    home_result = await crawl_page(crawler, base_url, "Home")
                    if home_result:
                        full_content += f"# Homepage\n{home_result.markdown}\n"

                    internal_links = []
                    external_links = []
                    if home_result:
                        internal_links = home_result.links.get('internal', [])
                        external_links = home_result.links.get('external', [])
                    all_links = internal_links + external_links

                    print(f"      🔗 Found {len(internal_links)} internal and {len(external_links)} external links.")

                    keywords = {
                        'contact': ['contato', 'contact', 'fale', 'atendimento'],
                        'about': ['sobre', 'about', 'quem-somos', 'institucional', 'empresa', 'imobiliaria'],
                        'services': ['servicos', 'services', 'produtos', 'products', 'imoveis']
                    }

                    social_keywords = {
                        'instagram': ['instagram.com'],
                        'facebook': ['facebook.com'],
                        'linktree': ['linktr.ee'],
                        'linkedin': ['linkedin.com']
                    }

                    def normalize_url(link_url, base):
                        if not link_url.startswith('http'):
                            if not base.endswith('/') and not link_url.startswith('/'):
                                return base + '/' + link_url
                            elif base.endswith('/') and link_url.startswith('/'):
                                return base + link_url[1:]
                            else:
                                return base + link_url
                        return link_url

                    unique_internal_urls = set()
                    priority_links = []
                    other_links = []
                    social_links_to_crawl = {}

                    for link in internal_links:
                        href = link.get('href', '')
                        if not href:
                            continue
                        full_url = normalize_url(href, base_url)
                        if full_url in unique_internal_urls or full_url.rstrip('/') == base_url.rstrip('/'):
                            continue
                        unique_internal_urls.add(full_url)
                        lower_href = href.lower()
                        is_priority = False
                        for key, words in keywords.items():
                            if any(w in lower_href for w in words):
                                is_priority = True
                                break
                        if is_priority:
                            priority_links.append(full_url)
                        else:
                            other_links.append(full_url)

                    for link in external_links:
                        href = link.get('href', '').lower()
                        for key, domains in social_keywords.items():
                            if key not in social_links_to_crawl and any(d in href for d in domains):
                                print(f"         📱 Found {key} link: {href}")
                                social_links_to_crawl[key] = link['href']

                    final_crawl_list = priority_links + other_links
                    print(f"      📋 Selected {len(final_crawl_list)} pages to crawl (Priority: {len(priority_links)})")

                    for j, link_url in enumerate(final_crawl_list):
                        print(f"      🕷️ Deep Crawling [{j+1}/{len(final_crawl_list)}]: {link_url}")
                        sub_result = await crawl_page(crawler, link_url, f"Page-{j+1}")
                        if sub_result:
                            full_content += f"\n# Page: {link_url}\n{sub_result.markdown}\n"

                    for key, link_url in social_links_to_crawl.items():
                        print(f"      🕷️ Social Crawling {key}: {link_url}")
                        sub_result = await crawl_page(crawler, link_url, f"Social-{key.capitalize()}")
                        if sub_result:
                            social_content = sub_result.markdown[:2000]
                            full_content += f"\n# Social Media ({key.capitalize()})\n{social_content}\n"
                            item[key] = link_url

                    try:
                        if base_url:
                            domain = base_url.replace('http://', '').replace('https://', '').split('/')[0]
                            hunter_emails = fetch_hunter_emails(domain)
                    except Exception as e:
                        print(f"      ⚠️ Failed to extract domain for Hunter: {e}")

                detected_tax_id = None
                detected_tech_stack = []
                if has_website and 'home_result' in locals() and home_result:
                    country_code_for_extraction = None
                    if search_country:
                        country_map = {
                            'Brazil': 'BR', 'Brasil': 'BR', 'USA': 'US', 'United States': 'US',
                            'France': 'FR', 'Germany': 'DE', 'UK': 'GB', 'United Kingdom': 'GB',
                            'Australia': 'AU', 'Mexico': 'MX', 'México': 'MX', 'Argentina': 'AR',
                            'Portugal': 'PT', 'Spain': 'ES', 'Italy': 'IT', 'Canada': 'CA'
                        }
                        country_code_for_extraction = country_map.get(search_country)
                    detected_tax_id = extract_tax_id(home_result.markdown, country_code_for_extraction)
                    if hasattr(home_result, 'html'):
                        detected_tech_stack = detect_tech_stack(home_result.html)

                system_context = "\n\n[SYSTEM DETECTED DATA (High Confidence)]\n"
                if detected_tax_id:
                    system_context += f"TAX ID: {detected_tax_id['type']} - {detected_tax_id['value']}\n"
                if detected_tech_stack:
                    system_context += f"TECH STACK: {', '.join(detected_tech_stack)}\n"
                full_content += system_context

                clean_profile = await analyze_with_gpt(full_content, item, hunter_emails, output_lang, search_country)
                if clean_profile:
                    if 'contact_details' in clean_profile:
                        if 'emails' in clean_profile['contact_details']:
                            raw_final_emails = clean_profile['contact_details']['emails']
                            validated_final_emails = []
                            for mail in raw_final_emails:
                                v_mail = verify_email_address(mail)
                                if v_mail:
                                    validated_final_emails.append(v_mail)
                            clean_profile['contact_details']['emails'] = validated_final_emails

                        if 'phones' in clean_profile['contact_details']:
                            phones = clean_profile['contact_details']['phones']
                            whatsapp_links = []
                            for p in phones:
                                wa = detect_whatsapp(p)
                                if wa:
                                    whatsapp_links.append({"phone": p, "link": wa})
                            if whatsapp_links:
                                clean_profile['contact_details']['whatsapp_verified'] = whatsapp_links

                    def clean_recursive(obj):
                        if isinstance(obj, dict):
                            for k, v in obj.items():
                                if isinstance(v, str) and "Pendente" in v:
                                    obj[k] = None
                                else:
                                    clean_recursive(v)
                        elif isinstance(obj, list):
                            for _it in obj:
                                clean_recursive(_it)

                    clean_recursive(clean_profile)

                    cnpja_data = None
                    tax_id_info = clean_profile.get('company_info', {}).get('tax_id')
                    if tax_id_info and isinstance(tax_id_info, dict):
                        tax_type = (tax_id_info.get('type') or '').upper()
                        tax_value = tax_id_info.get('value')
                        tax_country = (tax_id_info.get('country') or '').upper()
                        if tax_type == 'CNPJ' or tax_country == 'BR':
                            if tax_value:
                                cnpja_data = fetch_cnpja_data(tax_value)
                    clean_profile['cnpja_data'] = cnpja_data

                    final_leads.append(clean_profile)
                    print("      ✨ Lead successfully enriched and cleaned!")
                else:
                    print("      ⚠️ Could not generate clean profile, skipping.")

            # 2) Fica “tail -f” para consumir novas linhas até __END__
            for item in iter_jsonl_lines_follow(json_file):
                processed += 1
                i = processed - 1
                # ---- processamento do lead (mesma lógica do loop normal) ----
                base_url = item.get('website')
                hunter_emails = []
                has_website = False
                if base_url and "google.com" not in base_url and item.get('website') != "Não disponível":
                    has_website = True
                    if not base_url.startswith('http'):
                        base_url = 'http://' + base_url
                    print(f"[{processed}] Processing {item.get('nome_empresa')} ({base_url})...")
                else:
                    print(f"[{processed}] Processing {item.get('nome_empresa')} (No Website - Maps Data Only)...")

                full_content = ""

                if has_website:
                    home_result = await crawl_page(crawler, base_url, "Home")
                    if home_result:
                        full_content += f"# Homepage\n{home_result.markdown}\n"

                    internal_links = []
                    external_links = []
                    if home_result:
                        internal_links = home_result.links.get('internal', [])
                        external_links = home_result.links.get('external', [])
                    all_links = internal_links + external_links

                    print(f"      🔗 Found {len(internal_links)} internal and {len(external_links)} external links.")

                    keywords = {
                        'contact': ['contato', 'contact', 'fale', 'atendimento'],
                        'about': ['sobre', 'about', 'quem-somos', 'institucional', 'empresa', 'imobiliaria'],
                        'services': ['servicos', 'services', 'produtos', 'products', 'imoveis']
                    }

                    social_keywords = {
                        'instagram': ['instagram.com'],
                        'facebook': ['facebook.com'],
                        'linktree': ['linktr.ee'],
                        'linkedin': ['linkedin.com']
                    }

                    def normalize_url(link_url, base):
                        if not link_url.startswith('http'):
                            if not base.endswith('/') and not link_url.startswith('/'):
                                return base + '/' + link_url
                            elif base.endswith('/') and link_url.startswith('/'):
                                return base + link_url[1:]
                            else:
                                return base + link_url
                        return link_url

                    unique_internal_urls = set()
                    priority_links = []
                    other_links = []
                    social_links_to_crawl = {}

                    for link in internal_links:
                        href = link.get('href', '')
                        if not href:
                            continue
                        full_url = normalize_url(href, base_url)
                        if full_url in unique_internal_urls or full_url.rstrip('/') == base_url.rstrip('/'):
                            continue
                        unique_internal_urls.add(full_url)
                        lower_href = href.lower()
                        is_priority = False
                        for key, words in keywords.items():
                            if any(w in lower_href for w in words):
                                is_priority = True
                                break
                        if is_priority:
                            priority_links.append(full_url)
                        else:
                            other_links.append(full_url)

                    for link in external_links:
                        href = link.get('href', '').lower()
                        for key, domains in social_keywords.items():
                            if key not in social_links_to_crawl and any(d in href for d in domains):
                                print(f"         📱 Found {key} link: {href}")
                                social_links_to_crawl[key] = link['href']

                    final_crawl_list = priority_links + other_links
                    print(f"      📋 Selected {len(final_crawl_list)} pages to crawl (Priority: {len(priority_links)})")

                    for j, link_url in enumerate(final_crawl_list):
                        print(f"      🕷️ Deep Crawling [{j+1}/{len(final_crawl_list)}]: {link_url}")
                        sub_result = await crawl_page(crawler, link_url, f"Page-{j+1}")
                        if sub_result:
                            full_content += f"\n# Page: {link_url}\n{sub_result.markdown}\n"

                    for key, link_url in social_links_to_crawl.items():
                        print(f"      🕷️ Social Crawling {key}: {link_url}")
                        sub_result = await crawl_page(crawler, link_url, f"Social-{key.capitalize()}")
                        if sub_result:
                            social_content = sub_result.markdown[:2000]
                            full_content += f"\n# Social Media ({key.capitalize()})\n{social_content}\n"
                            item[key] = link_url

                    try:
                        if base_url:
                            domain = base_url.replace('http://', '').replace('https://', '').split('/')[0]
                            hunter_emails = fetch_hunter_emails(domain)
                    except Exception as e:
                        print(f"      ⚠️ Failed to extract domain for Hunter: {e}")

                detected_tax_id = None
                detected_tech_stack = []
                if has_website and 'home_result' in locals() and home_result:
                    country_code_for_extraction = None
                    if search_country:
                        country_map = {
                            'Brazil': 'BR', 'Brasil': 'BR', 'USA': 'US', 'United States': 'US',
                            'France': 'FR', 'Germany': 'DE', 'UK': 'GB', 'United Kingdom': 'GB',
                            'Australia': 'AU', 'Mexico': 'MX', 'México': 'MX', 'Argentina': 'AR',
                            'Portugal': 'PT', 'Spain': 'ES', 'Italy': 'IT', 'Canada': 'CA'
                        }
                        country_code_for_extraction = country_map.get(search_country)
                    detected_tax_id = extract_tax_id(home_result.markdown, country_code_for_extraction)
                    if hasattr(home_result, 'html'):
                        detected_tech_stack = detect_tech_stack(home_result.html)

                system_context = "\n\n[SYSTEM DETECTED DATA (High Confidence)]\n"
                if detected_tax_id:
                    system_context += f"TAX ID: {detected_tax_id['type']} - {detected_tax_id['value']}\n"
                if detected_tech_stack:
                    system_context += f"TECH STACK: {', '.join(detected_tech_stack)}\n"
                full_content += system_context

                clean_profile = await analyze_with_gpt(full_content, item, hunter_emails, output_lang, search_country)
                if clean_profile:
                    # (mesmo pós-processamento do fluxo normal)
                    if 'contact_details' in clean_profile:
                        if 'emails' in clean_profile['contact_details']:
                            raw_final_emails = clean_profile['contact_details']['emails']
                            validated_final_emails = []
                            for mail in raw_final_emails:
                                v_mail = verify_email_address(mail)
                                if v_mail:
                                    validated_final_emails.append(v_mail)
                            clean_profile['contact_details']['emails'] = validated_final_emails

                        if 'phones' in clean_profile['contact_details']:
                            phones = clean_profile['contact_details']['phones']
                            whatsapp_links = []
                            for p in phones:
                                wa = detect_whatsapp(p)
                                if wa:
                                    whatsapp_links.append({"phone": p, "link": wa})
                            if whatsapp_links:
                                clean_profile['contact_details']['whatsapp_verified'] = whatsapp_links

                    def clean_recursive(obj):
                        if isinstance(obj, dict):
                            for k, v in obj.items():
                                if isinstance(v, str) and "Pendente" in v:
                                    obj[k] = None
                                else:
                                    clean_recursive(v)
                        elif isinstance(obj, list):
                            for _it in obj:
                                clean_recursive(_it)

                    clean_recursive(clean_profile)

                    cnpja_data = None
                    tax_id_info = clean_profile.get('company_info', {}).get('tax_id')
                    if tax_id_info and isinstance(tax_id_info, dict):
                        tax_type = (tax_id_info.get('type') or '').upper()
                        tax_value = tax_id_info.get('value')
                        tax_country = (tax_id_info.get('country') or '').upper()
                        if tax_type == 'CNPJ' or tax_country == 'BR':
                            if tax_value:
                                cnpja_data = fetch_cnpja_data(tax_value)
                    clean_profile['cnpja_data'] = cnpja_data

                    final_leads.append(clean_profile)

                    print("      ✨ Lead successfully enriched and cleaned!")
                else:
                    print("      ⚠️ Could not generate clean profile, skipping.")

            # terminou stream -> salva o json final (array)
        else:
            for i, item in enumerate(items):
                base_url = item.get('website')
                hunter_emails = [] # Initialize here to avoid UnboundLocalError
            
            # Check if website is valid
            has_website = False
            if base_url and "google.com" not in base_url and item.get('website') != "Não disponível":
                has_website = True
                # Ensure protocol
                if not base_url.startswith('http'):
                    base_url = 'http://' + base_url
                print(f"[{i+1}/{len(items)}] Processing {item.get('nome_empresa')} ({base_url})...")
            else:
                print(f"[{i+1}/{len(items)}] Processing {item.get('nome_empresa')} (No Website - Maps Data Only)...")
            
            full_content = ""
            
            if has_website:
                # 1. Crawl Home
                home_result = await crawl_page(crawler, base_url, "Home")
            
                if home_result:
                    full_content += f"# Homepage\n{home_result.markdown}\n"
                    
                # 2. Find Interesting Links (About, Contact, Services) + Social Media
                internal_links = []
                external_links = []
                if home_result:
                    internal_links = home_result.links.get('internal', [])
                    external_links = home_result.links.get('external', [])
                all_links = internal_links + external_links
                
                print(f"      🔗 Found {len(internal_links)} internal and {len(external_links)} external links.")
                
                keywords = {
                    'contact': ['contato', 'contact', 'fale', 'atendimento'],
                    'about': ['sobre', 'about', 'quem-somos', 'institucional', 'empresa', 'imobiliaria'],
                    'services': ['servicos', 'services', 'produtos', 'products', 'imoveis']
                }
                
                social_keywords = {
                    'instagram': ['instagram.com'],
                    'facebook': ['facebook.com'],
                    'linktree': ['linktr.ee'],
                    'linkedin': ['linkedin.com']
                }

                # Helper to normalize URL
                def normalize_url(link_url, base):
                    if not link_url.startswith('http'):
                        if not base.endswith('/') and not link_url.startswith('/'):
                            return base + '/' + link_url
                        elif base.endswith('/') and link_url.startswith('/'):
                            return base + link_url[1:]
                        else:
                            return base + link_url
                    return link_url

                # Strategy: Collect ALL internal links, Prioritize Keywords, Limit to 10
                unique_internal_urls = set()
                priority_links = []
                other_links = []
                social_links_to_crawl = {}

                # 1. Process Internal Links
                for link in internal_links:
                    href = link.get('href', '')
                    if not href: continue
                    
                    full_url = normalize_url(href, base_url)
                    
                    # Avoid duplicates and self-reference
                    if full_url in unique_internal_urls or full_url.rstrip('/') == base_url.rstrip('/'):
                        continue
                        
                    unique_internal_urls.add(full_url)
                    
                    # Check Priority
                    lower_href = href.lower()
                    is_priority = False
                    for key, words in keywords.items():
                        if any(w in lower_href for w in words):
                            is_priority = True
                            break
                    
                    if is_priority:
                        priority_links.append(full_url)
                    else:
                        other_links.append(full_url)

                # 2. Process External Links (Social Only)
                for link in external_links:
                    href = link.get('href', '').lower()
                    for key, domains in social_keywords.items():
                        if key not in social_links_to_crawl and any(d in href for d in domains):
                            print(f"         📱 Found {key} link: {href}")
                            social_links_to_crawl[key] = link['href']

                # 3. Final Crawl List (Priority first, then others)
                # ATENÇÃO: sem limite -> pode aumentar MUITO o tempo/custo em sites grandes.
                final_crawl_list = priority_links + other_links
                
                print(f"      📋 Selected {len(final_crawl_list)} pages to crawl (Priority: {len(priority_links)})")

                # 4. Crawl Content Pages
                for i, link_url in enumerate(final_crawl_list):
                    print(f"      🕷️ Deep Crawling [{i+1}/{len(final_crawl_list)}]: {link_url}")
                    sub_result = await crawl_page(crawler, link_url, f"Page-{i+1}")
                    if sub_result:
                        full_content += f"\n# Page: {link_url}\n{sub_result.markdown}\n"

                # 4. Crawl Social Media (Carefully)
                for key, link_url in social_links_to_crawl.items():
                    print(f"      🕷️ Social Crawling {key}: {link_url}")
                    # Use a slightly more lenient config for socials if needed, or just standard
                    # Note: Instagram/FB might block or require login, but we try to get public bio
                    sub_result = await crawl_page(crawler, link_url, f"Social-{key.capitalize()}")
                    if sub_result:
                        # Limit content from social to avoid noise, just header/bio usually
                        social_content = sub_result.markdown[:2000] 
                        full_content += f"\n# Social Media ({key.capitalize()})\n{social_content}\n"
                        
                        # Update the social fields in the item directly
                        item[key] = link_url

                # 4. Hunter.io Email Search
                try:
                    if base_url:
                        # Simple domain extraction
                        domain = base_url.replace('http://', '').replace('https://', '').split('/')[0]
                        hunter_emails = fetch_hunter_emails(domain)
                except Exception as e:
                    print(f"      ⚠️ Failed to extract domain for Hunter: {e}")

            # 5. AI Analysis & Consolidation
            # Inject System Detected Data
            detected_tax_id = None
            detected_tech_stack = []
            
            if has_website and 'home_result' in locals() and home_result:
                 # Extract Tax ID from Markdown (based on country)
                 country_code_for_extraction = None
                 if search_country:
                     # Try to infer country code from country name
                     country_map = {
                         'Brazil': 'BR', 'Brasil': 'BR', 'USA': 'US', 'United States': 'US',
                         'France': 'FR', 'Germany': 'DE', 'UK': 'GB', 'United Kingdom': 'GB',
                         'Australia': 'AU', 'Mexico': 'MX', 'México': 'MX', 'Argentina': 'AR',
                         'Portugal': 'PT', 'Spain': 'ES', 'Italy': 'IT', 'Canada': 'CA'
                     }
                     country_code_for_extraction = country_map.get(search_country)
                 
                 detected_tax_id = extract_tax_id(home_result.markdown, country_code_for_extraction)
                 
                 # Detect Tech Stack from HTML (if available)
                 if hasattr(home_result, 'html'):
                     detected_tech_stack = detect_tech_stack(home_result.html)
            
            system_context = "\n\n[SYSTEM DETECTED DATA (High Confidence)]\n"
            if detected_tax_id:
                system_context += f"TAX ID: {detected_tax_id['type']} - {detected_tax_id['value']}\n"
            if detected_tech_stack:
                system_context += f"TECH STACK: {', '.join(detected_tech_stack)}\n"
            
            full_content += system_context

            clean_profile = await analyze_with_gpt(full_content, item, hunter_emails, output_lang, search_country)
            
            if clean_profile:
                # Post-process: Validate emails in the final profile
                if 'contact_details' in clean_profile:
                    # 1. Validate Emails
                    if 'emails' in clean_profile['contact_details']:
                        raw_final_emails = clean_profile['contact_details']['emails']
                        validated_final_emails = []
                        for mail in raw_final_emails:
                            v_mail = verify_email_address(mail)
                            if v_mail:
                                validated_final_emails.append(v_mail)
                        clean_profile['contact_details']['emails'] = validated_final_emails
                    
                    # 2. Detect WhatsApp
                    if 'phones' in clean_profile['contact_details']:
                        phones = clean_profile['contact_details']['phones']
                        whatsapp_links = []
                        for p in phones:
                            wa = detect_whatsapp(p)
                            if wa:
                                whatsapp_links.append({"phone": p, "link": wa})
                        
                        if whatsapp_links:
                            clean_profile['contact_details']['whatsapp_verified'] = whatsapp_links

                # 3. Recursive Cleanup of "Pendente"
                def clean_recursive(obj):
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            if isinstance(v, str) and "Pendente" in v:
                                obj[k] = None
                            else:
                                clean_recursive(v)
                    elif isinstance(obj, list):
                        for item in obj:
                            clean_recursive(item)
                
                clean_recursive(clean_profile)

                # 4. Fetch CNPJA Data if Brazil tax ID (CNPJ) exists
                cnpja_data = None
                tax_id_info = clean_profile.get('company_info', {}).get('tax_id')
                
                if tax_id_info and isinstance(tax_id_info, dict):
                    tax_type = (tax_id_info.get('type') or '').upper()
                    tax_value = tax_id_info.get('value')
                    tax_country = (tax_id_info.get('country') or '').upper()
                    
                    # Only call CNPJA API for Brazilian CNPJ
                    if tax_type == 'CNPJ' or tax_country == 'BR':
                        if tax_value:
                            cnpja_data = fetch_cnpja_data(tax_value)
                
                clean_profile['cnpja_data'] = cnpja_data

                final_leads.append(clean_profile)

                print("      ✨ Lead successfully enriched and cleaned!")
            else:
                print("      ⚠️ Could not generate clean profile, skipping.")

    # Save FINAL CLEAN JSON (array) - compatibilidade
    print(f"Saving CLEAN data to {final_file}...")
    with open(final_file, 'w', encoding='utf-8') as f:
        json.dump(final_leads, f, indent=2, ensure_ascii=False)
    
    # Clean up temporary input file (somente quando a entrada for .json, não .jsonl stream)
    # No modo stream, o arquivo .jsonl é usado como "fila" e pode ser útil manter para auditoria.
    try:
        if (not stream_mode) and os.path.exists(json_file):
            os.remove(json_file)
            print(f"Deleted temporary file: {json_file}")
    except Exception as e:
        print(f"⚠️ Could not delete temporary file: {e}")

    print("Done!")

if __name__ == "__main__":
    asyncio.run(main())
