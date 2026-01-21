# Google Maps Lead Scraper + Enrichment (Node + Python)

Este projeto coleta leads do Google Maps (Node.js + Playwright) e enriquece cada lead com crawl do site + consolidação via IA (Python + Crawl4AI + OpenAI) e opcionalmente busca emails no Hunter.io.

> **Formato de saída:** `final_leads.json` (sempre gerado pelo `enrich_leads.py` ao final do processamento).

## Checklist de requisitos (para rodar no CRM)

- Node.js (recomendado **18+**)
- Python (recomendado **3.10+**)
- Navegador do Playwright instalado
- Variáveis de ambiente configuradas (`.env`)

---

## 1) Instalação (Node.js)

### Dependências
As dependências Node estão em `package.json`:

- `playwright` (automação do browser / scraping)
- `minimist` (CLI args)
- `dotenv` (carregar `.env`)
- `openai` (para detecção de idioma via GPT no Node)

### Instalar
No diretório do projeto:

```bash
npm install
```

### Instalar browsers do Playwright
O Playwright precisa baixar os browsers:

```bash
npx playwright install
```

---

## 2) Instalação (Python)

### Criar e ativar ambiente virtual

**Windows (PowerShell):**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

**Windows (Git Bash):**

```bash
python -m venv .venv
source .venv/Scripts/activate
```

### Dependências Python
Atualmente não existe `requirements.txt` no repositório, então **crie e instale** com:

```bash
pip install crawl4ai openai python-dotenv requests email-validator
```

> Observação: `crawl4ai` usa navegador (Chromium) internamente; na prática ele costuma funcionar sem passos extras, mas dependendo do ambiente do CRM pode precisar de libs do sistema.

---

## 3) Configuração de ambiente (.env)

Crie um arquivo `.env` na raiz do projeto (não versionar):

```env
OPENAI_API_KEY=coloque_sua_chave_aqui
HUNTER_API_KEY=coloque_sua_chave_aqui
```

### Variáveis usadas

- `OPENAI_API_KEY`
  - Usada em:
    - `maps_scraper.js` (detectar idioma e configurações de busca via OpenAI)
    - `enrich_leads.py` (consolidar/enriquecer dados via OpenAI)

- `HUNTER_API_KEY`
  - Usada em `enrich_leads.py` para buscar emails do domínio via Hunter.
  - Se não estiver definida, o Hunter vai falhar/retornar vazio.

---

## 4) Arquivos importantes

- `maps_scraper.js`
  - Faz scraping do Google Maps via Playwright.
  - Gera um arquivo temporário `resultado_*.jsonl` (1 lead por linha) como “fila” para o worker Python.
  - Inicia o worker Python (`enrich_leads.py`) e escreve `__END__` ao final do arquivo.

- `enrich_leads.py`
  - Lê o `.jsonl` incrementalmente (stream) e processa lead a lead.
  - Faz crawl do website e páginas internas com `crawl4ai`.
  - (Opcional) consulta Hunter.io e valida emails.
  - Consolida os dados via OpenAI e salva no final em `final_leads.json`.

- `company_profile.json`
  - Contexto do seu CRM/empresa para gerar mensagens de outreach personalizadas.

- `proxies.txt`
  - Opcional: lista de proxies para Playwright (1 por linha).

---

## 5) Como executar

### Rodar o scraper (Node inicia o worker Python automaticamente)

Exemplo:

```bash
node maps_scraper.js --type "Dentiste" --city "Paris" --country "France" --lang "fr" --limit 2
```

Parâmetros comuns:

- `--type`: nicho/segmento (ex: "Dentiste")
- `--city`: cidade
- `--country`: país
- `--lang`: idioma de saída (ex: `pt`, `en`, `fr`)
- `--limit`: limite de leads

### Saída
- `final_leads.json`: gerado pelo Python ao terminar.

---

## 6) Notas de operação em ambiente de CRM

### Rate limit do Hunter (429)
O Hunter.io tem limite de requisições. O projeto já trata `429` com tentativas e espera (backoff). Mesmo assim, em listas enormes pode acontecer de alguns leads ficarem sem emails do Hunter.

### Headless vs Visual
- `maps_scraper.js`: Playwright normalmente roda visual.
- `enrich_leads.py`: `BrowserConfig(headless=False)` também roda visual.

Em servidores/CRM, provavelmente você vai querer **headless**.

### Arquivo temporário `resultado_*.jsonl`
Esse arquivo é o “buffer/fila” entre Node e Python. Se você quiser, dá pra excluir automaticamente após concluir (mas durante o stream é útil manter).

---

## 7) Checklist rápido (produção)

- [ ] Node 18+ instalado
- [ ] Python 3.10+ instalado
- [ ] `npm install`
- [ ] `npx playwright install`
- [ ] `.venv` criado + deps Python instaladas
- [ ] `.env` configurado (`OPENAI_API_KEY`, `HUNTER_API_KEY`)
- [ ] (Opcional) `proxies.txt` configurado
- [ ] `company_profile.json` revisado com os dados do CRM
