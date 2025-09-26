import Fastify from 'fastify';
import 'dotenv/config';
import OpenAI from 'openai';
import { createClient } from '@supabase/supabase-js';

const fastify = Fastify({ logger: true });
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
const supabase = (process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY)
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY)
  : null;

const FB_API_VERSION = 'v20.0';
const MODEL = process.env.BRAIN_MODEL || 'gpt-4.1';
const USE_LLM = String(process.env.BRAIN_USE_LLM || 'true').toLowerCase() === 'true';
const AGENT_URL = (process.env.AGENT_SERVICE_URL || '').replace(/\/+$/,'') + '/api/agent/actions';

const ALLOWED_TYPES = new Set(['GetCampaignStatus','PauseCampaign','UpdateAdSetDailyBudget']);

function genIdem() {
  const d = new Date();
  const p = (n)=>String(n).padStart(2,'0');
  return `think-${d.getFullYear()}${p(d.getMonth()+1)}${p(d.getDate())}-${p(d.getHours())}${p(d.getMinutes())}-${Math.random().toString(36).slice(2,8)}`;
}
const toInt = (v) => Number.isFinite(+v) ? Math.round(+v) : null;

async function getUserAccount(userAccountId) {
  if (!supabase) throw new Error('supabase not configured');
  const { data, error } = await supabase
    .from('user_accounts')
    .select('id, access_token, ad_account_id, page_id, telegram_id, telegram_bot_token, username, prompt3')
    .eq('id', userAccountId)
    .single();
  if (error) throw error;
  return data;
}

async function getLastReports(telegramId) {
  if (!supabase || !telegramId) return [];
  const { data, error } = await supabase
    .from('campaign_reports')
    .select('report_data, created_at')
    .eq('telegram_id', String(telegramId))
    .order('created_at', { ascending: false })
    .limit(3);
  if (error) {
    fastify.log.warn({ msg: 'load_last_reports_failed', error });
    return [];
  }
  return data || [];
}

async function fbGet(url) {
  const res = await fetch(url);
  const text = await res.text();
  if (!res.ok) throw new Error(`FB ${res.status}: ${text}`);
  try { return JSON.parse(text); } catch { return { raw: text }; }
}

async function fetchAccountStatus(adAccountId, accessToken) {
  const url = new URL(`https://graph.facebook.com/${FB_API_VERSION}/${adAccountId}`);
  url.searchParams.set('fields','account_status,disable_reason');
  url.searchParams.set('access_token', accessToken);
  return fbGet(url.toString());
}
async function fetchAdsets(adAccountId, accessToken) {
  const url = new URL(`https://graph.facebook.com/${FB_API_VERSION}/${adAccountId}/adsets`);
  url.searchParams.set('fields','id,name,daily_budget');
  url.searchParams.set('access_token', accessToken);
  return fbGet(url.toString());
}
async function fetchYesterdayInsights(adAccountId, accessToken) {
  const url = new URL(`https://graph.facebook.com/${FB_API_VERSION}/${adAccountId}/insights`);
  url.searchParams.set('fields','campaign_name,campaign_id,adset_name,adset_id,ad_name,ad_id,spend,actions,cpm,ctr,video_thruplay_watched_actions');
  url.searchParams.set('date_preset','yesterday');
  url.searchParams.set('level','ad');
  url.searchParams.set('access_token', accessToken);
  return fbGet(url.toString());
}

const SYSTEM_PROMPT = (clientPrompt) => [
  (clientPrompt || '').trim(),
  '',
  'Роль: Проанализируй входные данные (Think), сформируй список действий и одним вызовом отправь их исполнителю; если целевые не заданы, считай CPL=2$ (200 центов), daily budget=20$ (2000 центов); работай только с активными кампаниями; Increase/Decrease всегда ±20% от установленного дневного бюджета ad set (в центах), не превышай общий дневной лимит; Stop Campaign, если 3 дня подряд CPL > 5$.',
  '',
  'Тул: SendActions',
  '- POST https://agent2.performanteaiagency.com/api/agent/actions',
  '- Headers: Content-Type: application/json',
  '- BODY: { "idempotencyKey":"<uniq>", "source":"n8n", "account":{"userAccountId":"<UUID>"}, "actions":[{ "type":"<ActionName>", "params":{ } }]}',
  '',
  'Правила:',
  '- Бюджеты в центах; 1$=100; 20$→2000; +20% → 2400; −20% → 1600.',
  '- Увеличение/уменьшение только от установленного дневного бюджета ad set.',
  '- Если new > 5000 — перепроверь; > 10000 — не отправляй.',
  '- Работай только с ACTIVE (status=1).',
  '- CPL считай по onsite_conversion.total_messaging_connection.',
  '',
  'Доступные действия (ровно эти):',
  '- GetCampaignStatus {"campaign_id"}',
  '- PauseCampaign {"campaign_id","status":"PAUSED"}',
  '- UpdateAdSetDailyBudget {"adset_id","daily_budget"}',
  '',
  'Формат ответа: СТРОГО JSON: { "planNote": string, "actions": [ ... ] }'
].join('\n');

function validateAndNormalizeActions(actions) {
  if (!Array.isArray(actions)) throw new Error('actions must be array');
  const cleaned = [];
  for (const a of actions) {
    if (!a || typeof a !== 'object') continue;
    const type = String(a.type || '');
    if (!ALLOWED_TYPES.has(type)) continue;
    const params = a.params && typeof a.params === 'object' ? { ...a.params } : {};
    if (type === 'GetCampaignStatus') {
      if (!params.campaign_id) throw new Error('GetCampaignStatus: campaign_id required');
    }
    if (type === 'PauseCampaign') {
      if (!params.campaign_id) throw new Error('PauseCampaign: campaign_id required');
      params.status = 'PAUSED';
    }
    if (type === 'UpdateAdSetDailyBudget') {
      if (!params.adset_id) throw new Error('UpdateAdSetDailyBudget: adset_id required');
      const nb = toInt(params.daily_budget);
      if (nb === null) throw new Error('UpdateAdSetDailyBudget: daily_budget int cents required');
      if (nb > 10000) throw new Error('daily_budget > 10000 not allowed');
      params.daily_budget = nb;
    }
    cleaned.push({ type, params });
  }
  if (!cleaned.length) throw new Error('No valid actions');
  return cleaned;
}

async function sendActionsBatch(idem, userAccountId, actions) {
  const res = await fetch(AGENT_URL, {
    method: 'POST',
    headers: { 'content-type':'application/json' },
    body: JSON.stringify({
      idempotencyKey: idem,
      source: 'n8n',
      account: { userAccountId },
      actions
    })
  });
  const text = await res.text();
  let data; try { data = JSON.parse(text); } catch { data = { raw: text }; }
  if (!res.ok) throw new Error(`executor ${res.status}: ${text}`);
  return data;
}

async function sendTelegram(chatId, text, token) {
  if (!chatId) return false;
  const bot = token || process.env.TELEGRAM_FALLBACK_BOT_TOKEN;
  if (!bot) return false;
  const r = await fetch(`https://api.telegram.org/bot${bot}/sendMessage`, {
    method: 'POST',
    headers: { 'content-type':'application/json' },
    body: JSON.stringify({ chat_id: String(chatId), text, parse_mode:'Markdown', disable_web_page_preview: true })
  });
  return r.ok;
}

function buildReport({ date, accountStatus, insights, actions, lastReports }) {
  const statusLine = accountStatus?.account_status === 1
    ? `Аккаунт активен (ID: ${accountStatus?.id || '—'})`
    : `Аккаунт неактивен (причина: ${accountStatus?.disable_reason ?? '—'})`;

  const executed = actions?.length
    ? actions.map((a,i)=>`${i+1}. ${a.type} — ${JSON.stringify(a.params)}`).join('\n')
    : 'Действия по оптимизации не требовались';

  const last3 = (lastReports || [])
    .map((r,i)=>`Отчёт ${i+1}:\n${typeof r.report_data==='string' ? r.report_data : JSON.stringify(r.report_data)}`)
    .join('\n\n');

  const text = [
    `*Отчёт за ${date}*`,
    ``,
    `Статус кабинета: ${statusLine}`,
    ``,
    `Выполненные действия:`,
    executed,
    ``,
    `Аналитика (последние 3 отчёта):`,
    last3 || '—'
  ].join('\n');

  return text;
}

async function llmPlan(systemPrompt, userPayload) {
  const resp = await openai.chat.completions.create({
    model: MODEL,
    temperature: 0,
    messages: [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: JSON.stringify(userPayload) }
    ]
  });
  const txt = resp.choices?.[0]?.message?.content || '{}';
  let parsed;
  try { parsed = JSON.parse(txt); }
  catch { const m = txt.match(/\{[\s\S]*\}/); parsed = m ? JSON.parse(m[0]) : {}; }
  if (!parsed || !Array.isArray(parsed.actions)) throw new Error('LLM invalid output');
  return parsed;
}

// POST /api/brain/run  { idempotencyKey?, userAccountId, inputs?:{ dispatch?:boolean } }
fastify.post('/api/brain/run', async (request, reply) => {
  const started = Date.now();
  try {
    const { idempotencyKey, userAccountId, inputs } = request.body || {};
    if (!userAccountId) return reply.code(400).send({ error: 'userAccountId required' });

    const idem = idempotencyKey || genIdem();

    const ua = await getUserAccount(userAccountId);
    const [accountStatus, adsets, insights, lastReports] = await Promise.all([
      fetchAccountStatus(ua.ad_account_id, ua.access_token).catch(e=>({ error:String(e) })),
      fetchAdsets(ua.ad_account_id, ua.access_token).catch(e=>({ error:String(e) })),
      fetchYesterdayInsights(ua.ad_account_id, ua.access_token).catch(e=>({ error:String(e) })),
      getLastReports(ua.telegram_id)
    ]);

    const date = (insights?.data?.[0]?.date_start) || new Date().toISOString().slice(0,10);
    const system = SYSTEM_PROMPT(ua.prompt3 || '');
    const userPayload = {
      userAccountId,
      account_status: accountStatus,
      adsets: adsets?.data || [],
      yesterday_insights: insights?.data || [],
      last_reports: lastReports,
      defaults: { target_cpl_cents: 200, default_daily_budget_cents: 2000 }
    };

    const plan = USE_LLM ? await llmPlan(system, userPayload) : { planNote:'LLM disabled', actions: [] };
    const actions = validateAndNormalizeActions(plan.actions);

    let agentResponse = null;
    if (inputs?.dispatch) {
      agentResponse = await sendActionsBatch(idem, userAccountId, actions);
    }

    const reportText = buildReport({
      date, accountStatus, insights: insights?.data, actions: inputs?.dispatch ? actions : [],
      lastReports
    });

    // Save report/logs
    let execStatus = 'success';
    if (supabase) {
      try {
        await supabase.from('campaign_reports').insert({
          telegram_id: String(ua.telegram_id || ''),
          report_data: { text: reportText, date, planNote: plan.planNote, actions }
        });
      } catch (e) {
        fastify.log.warn({ msg:'save_campaign_report_failed', error:String(e) });
      }
      try {
        await supabase.from('brain_executions').insert({
          user_account_id: userAccountId,
          idempotency_key: idem,
          plan_json: plan,
          actions_json: actions,
          executor_response_json: agentResponse,
          report_text: reportText,
          status: execStatus,
          duration_ms: Date.now() - started
        });
      } catch (e) {
        fastify.log.warn({ msg:'save_brain_execution_failed', error:String(e) });
      }
    }

    // Send Telegram
    const sent = await sendTelegram(ua.telegram_id, reportText, ua.telegram_bot_token);

    return reply.send({
      idempotencyKey: idem,
      planNote: plan.planNote,
      actions,
      dispatched: !!inputs?.dispatch,
      agentResponse,
      telegramSent: sent
    });
  } catch (err) {
    request.log.error(err);
    return reply.code(500).send({ error:'brain_run_failed', details:String(err?.message || err) });
  }
});

// Старая совместимость: /api/brain/decide (только план, без FB fetch) — опционально, оставлено
fastify.post('/api/brain/decide', async (request, reply) => {
  try {
    const { idempotencyKey, userAccountId, goal, inputs } = request.body || {};
    if (!userAccountId) return reply.code(400).send({ error:'userAccountId required' });
    const system = SYSTEM_PROMPT(inputs?.client_prompt || '');
    const plan = USE_LLM ? await llmPlan(system, { goal, inputs }) : { planNote:'LLM disabled', actions: [] };
    const actions = validateAndNormalizeActions(plan.actions);
    return reply.send({ planNote: plan.planNote, actions, dispatched:false });
  } catch (err) {
    request.log.error(err);
    return reply.code(500).send({ error:'brain_decide_failed', details:String(err?.message || err) });
  }
});

const port = Number(process.env.BRAIN_PORT || 7080);
fastify.listen({ host:'0.0.0.0', port }).then(()=>fastify.log.info(`Brain listening on ${port}`)).catch(err=>{ fastify.log.error(err); process.exit(1); });
