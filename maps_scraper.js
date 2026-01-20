require("dotenv").config();
const { chromium } = require("playwright");
const fs = require("fs");
const minimist = require("minimist");
const OpenAI = require("openai");

// OpenAI Configuration for Language Detection
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

/**
 * Use GPT to detect language settings from search parameters
 * Returns: { lang, locale, countryCode, preposition, queryLang }
 */
async function detectLanguageWithGPT(type, city, country) {
  console.log("🧠 Detecting language with GPT...");

  try {
    const response = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      messages: [
        {
          role: "system",
          content:
            "You are a language detection expert. Analyze search terms and return language settings.",
        },
        {
          role: "user",
          content: `Analyze these search parameters and detect the appropriate language settings:
                
Search Type: "${type}"
City: "${city}"
Country: "${country}"

Return a JSON object with:
- lang: ISO 639-1 language code (e.g., "en", "fr", "ja", "pt")
- locale: Full locale for browser (e.g., "en-US", "fr-FR", "ja-JP")
- countryCode: ISO 3166-1 alpha-2 country code (e.g., "US", "FR", "JP")
- preposition: Query preposition in that language (e.g., "in" for English, "à" for French, "" for Japanese)
- queryLang: Human-readable language name

IMPORTANT: Detect the language from the actual text in 'type' and 'city', not just the country name.
For example, if type="歯科医", that's Japanese regardless of country.

Return ONLY the JSON object, no markdown or explanation.`,
        },
      ],
      temperature: 0.1,
      max_tokens: 200,
    });

    const content = response.choices[0].message.content.trim();
    // Parse JSON, handling potential markdown code blocks
    const jsonStr = content.replace(/```json\n?|\n?```/g, "").trim();
    const detected = JSON.parse(jsonStr);

    console.log(
      `   ✅ Detected: ${detected.queryLang} (${detected.lang}) for ${detected.countryCode}`,
    );
    return detected;
  } catch (error) {
    console.error("   ⚠️ GPT detection failed:", error.message);
    // Fallback to English
    return {
      lang: "en",
      locale: "en-US",
      countryCode: "US",
      preposition: "in",
      queryLang: "English",
    };
  }
}
async function getText(page, selector) {
  const el = page.locator(selector).first();
  if (await el.count()) {
    const txt = (await el.innerText()).trim();
    return txt || null;
  }
  return null;
}

async function getByAriaContains(page, containsText) {
  const loc = page.locator(`[aria-label*="${containsText}"]`).first();
  return (await loc.count()) ? loc : null;
}

async function typeLikeHuman(page, locatorOrSelector, text) {
  const input =
    typeof locatorOrSelector === "string"
      ? page.locator(locatorOrSelector)
      : locatorOrSelector;
  await input.click({ clickCount: 3 });
  await input.fill("");
  await page.waitForTimeout(150);
  await input.type(text, { delay: 40 }); // digita sozinho
}

// Language to Locale mapping
const LANG_TO_LOCALE = {
  pt: "pt-BR",
  en: "en-US",
  es: "es-ES",
  fr: "fr-FR",
  de: "de-DE",
  it: "it-IT",
  ja: "ja-JP",
  zh: "zh-CN",
  ko: "ko-KR",
  ar: "ar-SA",
  ru: "ru-RU",
};

// Language to query preposition mapping (for natural search queries)
const LANG_PREPOSITIONS = {
  pt: "em",
  en: "in",
  es: "en",
  fr: "à",
  de: "in",
  it: "a",
  ja: "",
  zh: "",
  ko: "",
  ar: "في",
  ru: "в",
};

// Multi-language labels for Google Maps interface elements
const LANG_LABELS = {
  pt: {
    address: ["Endereço", "Address"],
    phone: ["Telefone", "Phone"],
    website: ["Website", "Site"],
    hours: ["Horário", "Hours", "Open"],
    about: ["Sobre", "About"],
    description: ["Descrição", "Description"],
  },
  en: {
    address: ["Address"],
    phone: ["Phone"],
    website: ["Website"],
    hours: ["Hours", "Open"],
    about: ["About"],
    description: ["Description"],
  },
  fr: {
    address: ["Adresse", "Address"],
    phone: ["Téléphone", "Phone"],
    website: ["Site web", "Website", "Site"],
    hours: ["Horaires", "Heures", "Ouvert", "Hours", "Open"],
    about: ["À propos", "About"],
    description: ["Description"],
  },
  es: {
    address: ["Dirección", "Address"],
    phone: ["Teléfono", "Phone"],
    website: ["Sitio web", "Website"],
    hours: ["Horario", "Abierto", "Hours", "Open"],
    about: ["Acerca de", "About"],
    description: ["Descripción", "Description"],
  },
  de: {
    address: ["Adresse", "Address"],
    phone: ["Telefon", "Phone"],
    website: ["Website", "Webseite"],
    hours: ["Öffnungszeiten", "Geöffnet", "Hours", "Open"],
    about: ["Info", "About"],
    description: ["Beschreibung", "Description"],
  },
  it: {
    address: ["Indirizzo", "Address"],
    phone: ["Telefono", "Phone"],
    website: ["Sito web", "Website"],
    hours: ["Orari", "Aperto", "Hours", "Open"],
    about: ["Informazioni", "About"],
    description: ["Descrizione", "Description"],
  },
};

async function main() {
  console.log("🔍 Global Lead Scraper - CRM Ready\n");

  // Parse named arguments
  const args = minimist(process.argv.slice(2), {
    string: ["type", "city", "country", "lang", "address"],
    default: {
      limit: 30,
    },
  });

  let { type, city, country, lang, limit, address } = args;

  // Validate required parameters
  if (!type || !city || !country) {
    console.error("❌ Error: Missing required parameters.");
    console.error("\nUsage:");
    console.error(
      '  node maps_scraper.js --type "Dentist" --city "Paris" --country "France"',
    );
    console.error("\nOptional:");
    console.error('  --lang "fr"  (auto-detected if not provided)');
    console.error('  --address "Champs-Élysées"  (for more specific location)');
    process.exit(1);
  }

  // Auto-detect language if not provided
  let locale, preposition;
  if (!lang) {
    const detected = await detectLanguageWithGPT(type, city, country);
    lang = detected.lang;
    locale = detected.locale;
    preposition = detected.preposition;
  } else {
    locale = LANG_TO_LOCALE[lang] || "en-US";
    preposition = LANG_PREPOSITIONS[lang] || "in";
  }

  const MAX_RESULTS = parseInt(limit) || 30;

  // Construct search query
  let QUERY;
  if (address) {
    QUERY = `${type} ${preposition} ${address}, ${city}, ${country}`;
  } else {
    QUERY = `${type} ${preposition} ${city}, ${country}`;
  }

  console.log(`📍 Searching: "${QUERY}"`);
  console.log(`🌐 Language: ${lang} (Locale: ${locale})`);
  console.log(`📊 Limit: ${MAX_RESULTS}\n`);

  // Store metadata for enrichment
  const searchMetadata = {
    type,
    city,
    country,
    lang,
    address,
    limit: MAX_RESULTS,
  };

  // Carregar proxies (opcional)
  let proxies = [];
  try {
    if (fs.existsSync("proxies.txt")) {
      const proxyContent = fs.readFileSync("proxies.txt", "utf-8");
      proxies = proxyContent
        .split("\n")
        .map((p) => p.trim())
        .filter((p) => p && !p.startsWith("#"));
      if (proxies.length > 0) {
        console.log(`🛡️ Loaded ${proxies.length} proxies.`);
      }
    }
  } catch (e) {
    console.log("⚠️ Error reading proxies.txt:", e.message);
  }

  // Selecionar proxy aleatório
  let launchOptions = {
    headless: false,
    args: ["--start-maximized"],
  };

  if (proxies.length > 0) {
    const randomProxy = proxies[Math.floor(Math.random() * proxies.length)];
    console.log(`🛡️ Using Proxy: ${randomProxy}`);
    launchOptions.proxy = {
      server: randomProxy,
    };
  }

  const browser = await chromium.launch(launchOptions);
  const context = await browser.newContext({
    viewport: null,
    locale: locale,
  });

  const page = await context.newPage();
  await page.goto("https://www.google.com/maps", {
    waitUntil: "domcontentloaded",
  });

  // cookies (se aparecer)
  try {
    const agree = page.getByRole("button", {
      name: /Aceitar|I agree|Accept all|Aceitar tudo|Concordo/i,
    });
    if (await agree.count()) {
      console.log("🍪 Aceitando cookies...");
      await agree.first().click({ timeout: 5000 });
      await page.waitForTimeout(1000);
    }
  } catch (e) {
    console.log(
      "⚠️ Erro ao tentar aceitar cookies (pode não ter aparecido):",
      e.message,
    );
  }

  // 🔥 ELE MESMO DIGITA E PESQUISA
  const searchInput = page.locator(
    'input#searchboxinput, input[name="q"], input[aria-label="Search Google Maps"], input[aria-label="Pesquisar no Google Maps"]',
  );

  try {
    await searchInput.first().waitFor({ state: "visible", timeout: 30000 });
    console.log("✅ Campo de busca encontrado!");
  } catch (e) {
    console.log("❌ Campo de busca NÃO encontrado. Tentando debug...");
    // Tentar imprimir o HTML ou tirar screenshot se fosse possível ver
    throw e;
  }

  await typeLikeHuman(page, searchInput.first(), QUERY);
  await page.keyboard.press("Enter");

  // espera painel com resultados
  await page.waitForTimeout(3500);

  const resultsLocator = page.locator(
    'a[href^="https://www.google.com/maps/place"]',
  );
  await resultsLocator.first().waitFor({ timeout: 15000 });

  const panel = page.locator('div[role="feed"]');

  async function collectVisibleLinks() {
    const links = await resultsLocator.evaluateAll((els) =>
      els.map((a) => a.getAttribute("href")).filter(Boolean),
    );
    return [...new Set(links)];
  }

  let linksSet = new Set();
  let stableRounds = 0;

  while (linksSet.size < MAX_RESULTS && stableRounds < 5) {
    const newLinks = await collectVisibleLinks();
    const before = linksSet.size;
    newLinks.forEach((l) => linksSet.add(l));

    if (linksSet.size === before) stableRounds++;
    else stableRounds = 0;

    if (await panel.count()) {
      await panel.evaluate((el) => el.scrollBy(0, 1400));
    } else {
      await page.mouse.wheel(0, 1400);
    }

    await page.waitForTimeout(1500);
  }

  const links = Array.from(linksSet).slice(0, MAX_RESULTS);
  console.log(`Links coletados: ${links.length}`);

  // Função para extrair dados do site da empresa
  async function scrapeWebsiteData(context, url) {
    if (!url || url.includes("google.com")) return {};

    // Garantir protocolo
    if (!url.startsWith("http")) {
      url = "http://" + url;
    }

    const page = await context.newPage();
    const data = {
      email: null,
      facebook: null,
      instagram: null,
      twitter: null,
      linkedin: null,
      youtube: null,
      tiktok: null,
      whatsapp: null,
      linktree: null,
      phones_web: null,
    };

    async function extractFromContent(pg) {
      try {
        const content = await pg.content();
        const text = await pg.locator("body").innerText(); // Usar innerText para evitar tags ocultas/scripts

        // Extrair Emails
        const emailMatch = content.match(
          /[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,6}/g,
        );
        if (emailMatch) {
          const validEmails = [...new Set(emailMatch)].filter(
            (e) => !e.match(/\.(png|jpg|jpeg|gif|webp|svg|css|js)$/i),
          );
          if (validEmails.length > 0) data.email = validEmails.join(", ");
        }

        // Extract Phones (UNIVERSAL INTERNATIONAL REGEX)
        // Matches: +1-555-123-4567, (555) 123-4567, 01 23 45 67 89, 03-1234-5678, etc.
        const phoneRegex =
          /(?:\+?\d{1,4}[-.\s]?)?(?:\(?\d{1,5}\)?[-.\s]?)?\d{2,5}[-.\s]?\d{2,5}[-.\s]?\d{2,5}/g;
        const phoneMatch = text.match(phoneRegex);
        if (phoneMatch) {
          // Clean and filter - minimum 7 digits to be a valid phone
          const validPhones = [...new Set(phoneMatch)]
            .map((p) => p.trim())
            .filter((p) => {
              const digitsOnly = p.replace(/\D/g, "");
              return digitsOnly.length >= 7 && digitsOnly.length <= 15;
            });
          if (validPhones.length > 0) data.phones_web = validPhones.join(" | ");
        }
      } catch (e) {}
      return null;
    }

    try {
      console.log(`   🌐 Visitando site: ${url}`);
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 15000 });

      // Tentar aceitar cookies
      try {
        const frames = page.frames();
      } catch (e) {}

      // 1. Extração na Home
      await extractFromContent(page);

      // Extrair Redes Sociais (Links)
      const links = await page.evaluate(() => {
        return Array.from(document.querySelectorAll("a")).map((a) => a.href);
      });

      links.forEach((link) => {
        if (link.includes("facebook.com") && !link.includes("sharer"))
          data.facebook = link;
        if (link.includes("instagram.com")) data.instagram = link;
        if (link.includes("twitter.com") || link.includes("x.com"))
          data.twitter = link;
        if (link.includes("linkedin.com")) data.linkedin = link;
        if (link.includes("youtube.com")) data.youtube = link;
        if (link.includes("tiktok.com")) data.tiktok = link;
        if (link.includes("wa.me") || link.includes("api.whatsapp.com"))
          data.whatsapp = link;
        if (link.includes("linktr.ee")) data.linktree = link;
      });

      // 2. Se não achou email, procurar página de contato
      if (!data.email) {
        const contactLink = links.find((l) =>
          /contato|contact|fale-conosco|sobre|about/i.test(l),
        );
        if (contactLink) {
          console.log(
            `   🔎 Email não encontrado na home. Tentando: ${contactLink}`,
          );
          try {
            await page.goto(contactLink, {
              waitUntil: "domcontentloaded",
              timeout: 10000,
            });
            await extractFromContent(page);
          } catch (e) {
            console.log(`   ⚠️ Falha ao acessar página de contato.`);
          }
        }
      }

      // 3. Fallback: Tentar redes sociais se ainda não tiver email
      if (!data.email) {
        let socialUrl = data.facebook || data.instagram;
        if (socialUrl) {
          console.log(
            `   🔎 Email ainda não encontrado. Tentando rede social: ${socialUrl}`,
          );
          try {
            await page.goto(socialUrl, {
              waitUntil: "domcontentloaded",
              timeout: 10000,
            });
            await page.waitForTimeout(2000); // Esperar carregar um pouco
            await extractFromContent(page);
          } catch (e) {
            console.log(`   ⚠️ Falha ao acessar rede social.`);
          }
        }
      }
    } catch (e) {
      console.log(`   ⚠️ Erro ao acessar site: ${e.message}`);
    } finally {
      await page.close();
    }
    return data;
  }

  const items = [];

  for (let i = 0; i < links.length; i++) {
    const url = links[i];
    console.log(`(${i + 1}/${links.length}) Abrindo: ${url}`);

    try {
      await page.goto(url, { waitUntil: "domcontentloaded" });
      await page.waitForTimeout(2000);

      const name = await getText(page, "h1");

      // Tentar seletores para categoria
      let category = await getText(
        page,
        'button[jsaction*="pane.rating.category"]',
      );
      if (!category) {
        category = await getText(page, 'button[jsaction*="category"]');
      }
      // ====== UNIVERSAL LANGUAGE-AGNOSTIC SELECTORS ======
      // Use data-item-id attributes (work in ANY language!)
      // These are Google's internal identifiers that don't change with locale

      // Address - data-item-id="address"
      let address = null;
      try {
        const addressEl = page
          .locator('button[data-item-id="address"], a[data-item-id="address"]')
          .first();
        if (await addressEl.count()) {
          address =
            (await addressEl.getAttribute("aria-label")) ||
            (await addressEl.innerText());
        }
      } catch (e) {}

      // Phone - data-item-id starts with "phone:"
      let phone = null;
      try {
        const phoneEl = page
          .locator('button[data-item-id^="phone:"], a[data-item-id^="phone:"]')
          .first();
        if (await phoneEl.count()) {
          phone =
            (await phoneEl
              .getAttribute("data-item-id")
              ?.replace("phone:tel:", "")) ||
            (await phoneEl.getAttribute("aria-label")) ||
            (await phoneEl.innerText());
        }
      } catch (e) {}

      // Website - data-item-id="authority"
      let website = null;
      try {
        const websiteEl = page.locator('a[data-item-id="authority"]').first();
        if (await websiteEl.count()) {
          // First try to get the href directly (cleanest source)
          let rawWebsite =
            (await websiteEl.getAttribute("href")) ||
            (await websiteEl.getAttribute("aria-label")) ||
            (await websiteEl.innerText());

          // Clean the URL: remove prefixes like "Site web:", "Website:", etc.
          if (rawWebsite) {
            // Extract URL pattern or clean prefix
            const urlMatch = rawWebsite.match(/https?:\/\/[^\s]+/i);
            if (urlMatch) {
              website = urlMatch[0];
            } else {
              // Remove common prefixes in any language
              website = rawWebsite
                .replace(
                  /^(Site web|Website|Sitio web|Sito web|Webseite|Site):\s*/i,
                  "",
                )
                .trim();
              // Add http if missing
              if (website && !website.startsWith("http")) {
                website = "http://" + website;
              }
            }
          }
        }
      } catch (e) {}

      // Extrair Horário de Funcionamento
      let openingHours = null;
      try {
        // Tenta pegar o texto visível primeiro (ex: "Aberto ⋅ Fecha às 18:00")
        const hoursDiv = page.locator(
          'div[aria-label*="Horário de funcionamento"], div[aria-label*="Open"], div.OqCZI',
        );
        if (await hoursDiv.count()) {
          const aria = await hoursDiv.first().getAttribute("aria-label");
          const text = await hoursDiv.first().innerText();

          // Se o aria-label tiver números, é bom. Se não, usa o texto.
          if (aria && aria.match(/\d/)) {
            openingHours = aria;
          } else if (text && text.match(/\d/)) {
            openingHours = text.replace(/\n/g, " | ");
          } else {
            // Se não conseguiu no simples, tenta clicar para expandir (se for botão)
            const hoursBtn = page.locator('button[data-item-id="oh"]');
            if (await hoursBtn.count()) {
              await hoursBtn.click();
              await page.waitForTimeout(1000);
              const fullHours = page.locator(
                'div[aria-label*="Horário"], div.t39EBf',
              ); // Tenta pegar a tabela
              if (await fullHours.count()) {
                openingHours =
                  (await fullHours.first().getAttribute("aria-label")) ||
                  (await fullHours.first().innerText());
              }
            }
          }
        }

        if (openingHours) {
          openingHours = openingHours
            .replace("Horário de funcionamento: ", "")
            .replace(/;/g, " | ");
        }
      } catch (e) {
        console.log("Erro ao extrair horários:", e.message);
      }

      // Extrair rating se disponível
      const ratingElement = page
        .locator('div.F7nice span[aria-hidden="true"]')
        .first();
      const rating = (await ratingElement.count())
        ? await ratingElement.innerText()
        : null;

      // Extrair número de avaliações
      let reviews = null;
      const reviewsElement = page
        .locator('button[jsaction*="pane.rating.moreReviews"] span')
        .first();
      if (await reviewsElement.count()) {
        reviews = await reviewsElement.innerText();
      } else {
        const possibleReviews = page
          .locator('span:has-text("("):has-text(")")')
          .first();
        if (await possibleReviews.count()) {
          reviews = await possibleReviews.innerText();
        }
      }

      // Extrair texto de algumas avaliações (Top 3)
      let reviewTexts = [];
      try {
        const moreReviewsBtn = page.locator(
          'button[aria-label*="Comentários"], button[aria-label*="Reviews"]',
        );
        if (await moreReviewsBtn.count()) {
          await moreReviewsBtn.first().click();
          await page.waitForTimeout(1500);
          const reviewEls = page.locator("div.MyEned span.wiI7pd");
          const count = await reviewEls.count();
          for (let r = 0; r < Math.min(count, 3); r++) {
            const txt = await reviewEls.nth(r).innerText();
            if (txt) reviewTexts.push(txt.replace(/\n/g, " "));
          }
          // Voltar para visão geral (opcional, mas bom para manter estado se precisasse)
        }
      } catch (e) {
        // Ignorar erro ao pegar reviews
      }

      // Limpar dados básicos
      const cleanPhone = phone
        ? phone.replace(/^Telefone:\s*/i, "").trim()
        : null;
      const cleanAddress = address
        ? address.replace(/^Endereço:\s*/i, "").trim()
        : null;
      const cleanWebsite = website
        ? website.replace(/^Website:\s*/i, "").trim()
        : null;

      // --- DEEP MAPS EXTRACTION ---

      // 1. Claim Status (Reivindicar empresa)
      let isClaimed = true;
      try {
        const claimBtn = page.locator(
          'a[aria-label*="Reivindicar esta empresa"], button[aria-label*="Reivindicar esta empresa"], a:has-text("Reivindicar esta empresa"), button:has-text("Reivindicar esta empresa")',
        );
        if ((await claimBtn.count()) > 0) {
          isClaimed = false;
        }
      } catch (e) {}

      // 2. Plus Code (Localização precisa)
      let plusCode = null;
      try {
        const plusCodeBtn = page.locator('button[aria-label*="Plus Code"]');
        if (await plusCodeBtn.count()) {
          plusCode = await plusCodeBtn.getAttribute("aria-label");
          plusCode = plusCode.replace("Plus Code: ", "").trim();
        }
      } catch (e) {}

      // 3. Located In (Dentro de shopping/prédio)
      let locatedIn = null;
      try {
        const locatedInBtn = page.locator(
          'button[aria-label*="Localizado em"], button[aria-label*="Located in"]',
        );
        if (await locatedInBtn.count()) {
          locatedIn = await locatedInBtn.getAttribute("aria-label");
          locatedIn = locatedIn
            .replace(/Localizado em: |Located in: /i, "")
            .trim();
        }
      } catch (e) {}

      // 4. Description (Resumo do Google)
      let googleDescription = null;
      try {
        const descEl = page.locator('div[aria-label*="Descrição"], div.PYvSYb');
        if (await descEl.count()) {
          googleDescription = await descEl.first().innerText();
        }
      } catch (e) {}

      // 5. Action Buttons (Reservar, Pedir, Menu)
      let availableActions = [];
      try {
        const actionButtons = ["Reservar", "Fazer pedido", "Menu", "Agendar"];
        for (const action of actionButtons) {
          const btn = page.locator(
            `button:has-text("${action}"), a:has-text("${action}")`,
          );
          if ((await btn.count()) > 0) {
            availableActions.push(action);
          }
        }
      } catch (e) {}

      // 6. Deep Attributes (Click "Sobre" Tab)
      let googleAttributes = {};
      try {
        // Try to find and click the "Sobre" (About) tab
        const aboutTab = page
          .locator(
            'button[aria-label*="Sobre"], div[role="tab"]:has-text("Sobre")',
          )
          .first();
        if ((await aboutTab.count()) > 0) {
          await aboutTab.click();
          await page.waitForTimeout(1500); // Wait for panel to load

          // Extract attributes from the panel
          // Usually they are in sections like "Comodidades", "Acessibilidade"
          // We will grab all text from the main panel area
          const attributesPanel = page.locator('div[aria-label*="Sobre"]');
          if ((await attributesPanel.count()) > 0) {
            const textContent = await attributesPanel.innerText();
            // Simple parsing: Store the raw text for AI to process,
            // or try to split by newlines if it's structured.
            // For now, let's store the raw text to be safe and let AI parse it.
            googleAttributes["raw_about_text"] = textContent;
          }
        }
      } catch (e) {
        console.log(`Erro ao navegar na aba Sobre: ${e.message}`);
      }

      // -----------------------------

      // 7. GPS Coordinates (from URL)
      let latitude = null;
      let longitude = null;
      try {
        const currentUrl = page.url();
        // URL pattern: ...!3d-23.6002614!4d-46.6722955...
        const latMatch = currentUrl.match(/!3d([-0-9.]+)/);
        const longMatch = currentUrl.match(/!4d([-0-9.]+)/);

        if (latMatch && latMatch[1]) latitude = latMatch[1];
        if (longMatch && longMatch[1]) longitude = longMatch[1];
      } catch (e) {
        console.log("Erro ao extrair GPS:", e.message);
      }

      // -----------------------------

      // Deep Scraping (Site) - DESATIVADO NO NODE (Será feito pelo Python Crawl4AI)
      // let websiteData = {};
      // if (cleanWebsite) {
      //     websiteData = await scrapeWebsiteData(context, cleanWebsite);
      // }

      items.push({
        nome_empresa: name || "Não disponível",
        categoria: category || "Não disponível",
        endereco: cleanAddress || "Não disponível",
        telefone: cleanPhone || "Não disponível",
        website: cleanWebsite || "Não disponível",
        avaliacao: rating || "Não disponível",
        numero_avaliacoes: reviews || "Não disponível",
        comentarios: reviewTexts.join(" | ") || "Não disponível",
        horario_funcionamento: openingHours || "Não disponível",
        email: "Pendente (Python)", // websiteData.email || "Não disponível",
        telefones_site: "Pendente (Python)", // websiteData.phones_web || "Não disponível",
        facebook: "Pendente (Python)", // websiteData.facebook || "Não disponível",
        instagram: "Pendente (Python)", // websiteData.instagram || "Não disponível",
        twitter: "Pendente (Python)", // websiteData.twitter || "Não disponível",
        linkedin: "Pendente (Python)", // websiteData.linkedin || "Não disponível",
        latitude: latitude || null,
        longitude: longitude || null,
        youtube: "Pendente (Python)", // websiteData.youtube || "Não disponível",
        tiktok: "Pendente (Python)", // websiteData.tiktok || "Não disponível",
        whatsapp: "Pendente (Python)", // websiteData.whatsapp || "Não disponível",
        linktree: "Pendente (Python)", // websiteData.linktree || "Não disponível",
        url_maps: page.url(),
        query_busca: QUERY,
        google_maps_details: {
          is_claimed: isClaimed,
          plus_code: plusCode,
          located_in: locatedIn,
          google_description: googleDescription,
          available_actions: availableActions,
          google_attributes: googleAttributes,
        },
      });
    } catch (error) {
      console.log(`❌ Erro ao processar ${url}: ${error.message}`);
    }

    await page.waitForTimeout(800);
  }

  // Gerar nome do arquivo seguro
  const safeName = QUERY.replace(/[^\w\s]+/g, "")
    .replace(/\s+/g, "_")
    .slice(0, 60);
  const timestamp = new Date().toISOString().slice(0, 19).replace(/[:.]/g, "-");
  const fileName = `resultado_${safeName}_${timestamp}.json`;

  // Salvar resultados em JSON (Temporário para o Python)
  fs.writeFileSync(fileName, JSON.stringify(items, null, 2), "utf-8");

  console.log(`\n✅ ${items.length} empresas encontradas!`);
  console.log(`📄 Dados temporários salvos em: ${fileName}`);

  await browser.close();

  // Executar enriquecimento com Python (Crawl4AI)
  console.log(
    "\n🚀 Iniciando enriquecimento de dados com Crawl4AI (Python)...",
  );
  const { spawn } = require("child_process");

  // Usar caminho relativo explícito para Windows
  const pythonExecutable = ".\\.venv\\Scripts\\python.exe";
  const pythonArgs = ["enrich_leads.py", fileName, lang, country];

  console.log(`   Executing: ${pythonExecutable} ${pythonArgs.join(" ")}`);

  const pythonProcess = spawn(pythonExecutable, pythonArgs, {
    stdio: "inherit",
  });

  pythonProcess.on("close", (code) => {
    if (code === 0) {
      console.log("✨ Processo finalizado com sucesso!");
    } else {
      console.error(`❌ Python encerrou com código de erro: ${code}`);
    }
  });
}

// Executar o scraper
main().catch((err) => {
  console.error("\n❌ Erro:", err.message);
  process.exit(1);
});
