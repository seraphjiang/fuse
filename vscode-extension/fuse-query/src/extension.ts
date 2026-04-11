import * as vscode from 'vscode';
import * as http from 'http';
import * as https from 'https';

// ── Helpers ──

function getConfig() {
  const cfg = vscode.workspace.getConfiguration('fuse');
  return {
    serverUrl: cfg.get<string>('serverUrl', 'http://localhost:3000'),
    defaultFormat: cfg.get<string>('defaultFormat', 'sql'),
  };
}

function request(url: string, method: string, body?: string): Promise<any> {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const mod = u.protocol === 'https:' ? https : http;
    const req = mod.request(u, { method, headers: body ? { 'Content-Type': 'application/json' } : {} }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); } catch { resolve(data); }
      });
    });
    req.on('error', reject);
    if (body) { req.write(body); }
    req.end();
  });
}

function getFormat(langId: string, cfg: ReturnType<typeof getConfig>): string {
  if (langId === 'fuseppl') { return 'ppl'; }
  if (langId === 'fusesql') { return 'sql'; }
  return cfg.defaultFormat;
}

function getQueryText(editor: vscode.TextEditor): string {
  const sel = editor.selection;
  return sel.isEmpty ? editor.document.getText() : editor.document.getText(sel);
}

// ── Results Panel ──

let resultsPanel: vscode.WebviewPanel | undefined;

function showResults(title: string, content: string) {
  if (!resultsPanel) {
    resultsPanel = vscode.window.createWebviewPanel('fuseResults', 'Fuse Results', vscode.ViewColumn.Beside, { enableScripts: false });
    resultsPanel.onDidDispose(() => { resultsPanel = undefined; });
  }
  resultsPanel.title = title;
  resultsPanel.webview.html = `<!DOCTYPE html><html><head><style>
    body{font-family:var(--vscode-font-family);color:var(--vscode-foreground);background:var(--vscode-editor-background);padding:12px;font-size:13px}
    table{border-collapse:collapse;width:100%}
    th{text-align:left;padding:6px 10px;border-bottom:2px solid var(--vscode-panel-border);font-weight:600;position:sticky;top:0;background:var(--vscode-editor-background)}
    td{padding:4px 10px;border-bottom:1px solid var(--vscode-panel-border)}
    tr:hover td{background:var(--vscode-list-hoverBackground)}
    .meta{color:var(--vscode-descriptionForeground);font-size:11px;margin-bottom:8px}
    .error{color:var(--vscode-errorForeground);white-space:pre-wrap}
    pre{white-space:pre-wrap;font-family:var(--vscode-editor-font-family)}
  </style></head><body>${content}</body></html>`;
}

function renderTable(res: any): string {
  const cols: string[] = res.columns || [];
  const rows: any[][] = res.rows || [];
  const meta = res.metadata || {};
  let html = `<div class="meta">${rows.length} rows`;
  if (meta.elapsed_ms) { html += ` · ${meta.elapsed_ms}ms`; }
  html += '</div>';
  html += `<table><tr>${cols.map((c: string) => `<th>${esc(c)}</th>`).join('')}</tr>`;
  html += rows.map((r: any[]) => `<tr>${r.map(v => `<td>${esc(String(v ?? 'NULL'))}</td>`).join('')}</tr>`).join('');
  html += '</table>';
  return html;
}

function esc(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// ── Commands ──

async function runQuery() {
  const editor = vscode.window.activeTextEditor;
  if (!editor) { return; }
  const cfg = getConfig();
  const query = getQueryText(editor);
  const format = getFormat(editor.document.languageId, cfg);

  try {
    const res = await request(`${cfg.serverUrl}/api/fuse/query`, 'POST', JSON.stringify({ query, format }));
    if (res.error) {
      showResults('Fuse Error', `<div class="error">${esc(res.error)}</div>`);
    } else {
      showResults('Fuse Results', renderTable(res));
      historyProvider.addEntry(query, format, (res.rows || []).length);
    }
  } catch (e: any) {
    vscode.window.showErrorMessage(`Fuse: ${e.message}`);
  }
}

async function explainQuery() {
  const editor = vscode.window.activeTextEditor;
  if (!editor) { return; }
  const cfg = getConfig();
  const query = getQueryText(editor);
  const format = getFormat(editor.document.languageId, cfg);

  try {
    const res = await request(`${cfg.serverUrl}/api/fuse/query/explain`, 'POST', JSON.stringify({ query, format }));
    if (res.plan_tree) {
      showResults('Fuse Explain', `<pre>${esc(JSON.stringify(res.plan_tree, null, 2))}</pre>`);
    } else {
      showResults('Fuse Explain', `<pre>${esc(res.plan || JSON.stringify(res, null, 2))}</pre>`);
    }
  } catch (e: any) {
    vscode.window.showErrorMessage(`Fuse: ${e.message}`);
  }
}

async function validateQuery() {
  const editor = vscode.window.activeTextEditor;
  if (!editor) { return; }
  const cfg = getConfig();
  const query = getQueryText(editor);
  const format = getFormat(editor.document.languageId, cfg);

  try {
    const res = await request(`${cfg.serverUrl}/api/fuse/query/validate`, 'POST', JSON.stringify({ query, format }));
    if (res.valid) {
      vscode.window.showInformationMessage('✓ Query is valid');
    } else {
      vscode.window.showWarningMessage(`Query invalid: ${res.error}`);
    }
  } catch (e: any) {
    vscode.window.showErrorMessage(`Fuse: ${e.message}`);
  }
}

// ── Datasource Tree View ──

class DatasourceItem extends vscode.TreeItem {
  constructor(
    public readonly label: string,
    public readonly collapsibleState: vscode.TreeItemCollapsibleState,
    public readonly dsId?: string,
    public readonly table?: string,
  ) {
    super(label, collapsibleState);
    if (!dsId) {
      this.iconPath = new vscode.ThemeIcon('database');
    } else if (!table) {
      this.iconPath = new vscode.ThemeIcon('table');
      this.contextValue = 'table';
    } else {
      this.iconPath = new vscode.ThemeIcon('symbol-field');
    }
  }
}

class DatasourceProvider implements vscode.TreeDataProvider<DatasourceItem> {
  private _onDidChange = new vscode.EventEmitter<void>();
  readonly onDidChangeTreeData = this._onDidChange.event;

  refresh() { this._onDidChange.fire(); }

  getTreeItem(el: DatasourceItem) { return el; }

  async getChildren(el?: DatasourceItem): Promise<DatasourceItem[]> {
    const cfg = getConfig();
    try {
      if (!el) {
        const ds = await request(`${cfg.serverUrl}/api/fuse/datasources`, 'GET');
        return (ds || []).map((d: any) => new DatasourceItem(
          d.name || d.id, vscode.TreeItemCollapsibleState.Collapsed, d.id || d.name
        ));
      }
      if (el.dsId && !el.table) {
        const schemas = await request(`${cfg.serverUrl}/api/fuse/datasources/${el.dsId}/schemas`, 'GET');
        return (schemas || []).map((t: any) => {
          const name = typeof t === 'string' ? t : t.name;
          return new DatasourceItem(name, vscode.TreeItemCollapsibleState.Collapsed, el.dsId, name);
        });
      }
      if (el.dsId && el.table) {
        const fields = await request(`${cfg.serverUrl}/api/fuse/datasources/${el.dsId}/schemas/${el.table}/fields`, 'GET');
        return (fields || []).map((f: any) => {
          const name = typeof f === 'string' ? f : `${f.name}: ${f.field_type || f.type || '?'}`;
          return new DatasourceItem(name, vscode.TreeItemCollapsibleState.None);
        });
      }
    } catch { /* server unavailable */ }
    return [];
  }
}

// ── Query History Tree View ──

interface HistoryEntry { query: string; format: string; rows: number; time: number; }

class HistoryItem extends vscode.TreeItem {
  constructor(public readonly entry: HistoryEntry) {
    super(entry.query.substring(0, 60).replace(/\n/g, ' '), vscode.TreeItemCollapsibleState.None);
    this.description = `${entry.rows} rows · ${entry.format}`;
    this.tooltip = entry.query;
    this.iconPath = new vscode.ThemeIcon('history');
    this.command = { command: 'fuse.insertQuery', title: 'Insert', arguments: [entry.query] };
  }
}

class HistoryProvider implements vscode.TreeDataProvider<HistoryItem> {
  private _onDidChange = new vscode.EventEmitter<void>();
  readonly onDidChangeTreeData = this._onDidChange.event;
  private entries: HistoryEntry[] = [];

  addEntry(query: string, format: string, rows: number) {
    this.entries.unshift({ query, format, rows, time: Date.now() });
    if (this.entries.length > 50) { this.entries.length = 50; }
    this._onDidChange.fire();
  }

  getTreeItem(el: HistoryItem) { return el; }
  getChildren(): HistoryItem[] { return this.entries.map(e => new HistoryItem(e)); }
}

const historyProvider = new HistoryProvider();

// ── IntelliSense ──

class FuseCompletionProvider implements vscode.CompletionItemProvider {
  private keywords = [
    'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'JOIN', 'LEFT', 'RIGHT',
    'INNER', 'CROSS', 'ON', 'GROUP', 'BY', 'ORDER', 'ASC', 'DESC', 'HAVING',
    'LIMIT', 'OFFSET', 'UNION', 'ALL', 'DISTINCT', 'AS', 'CASE', 'WHEN', 'THEN',
    'ELSE', 'END', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE', 'CREATE',
    'DROP', 'VIEW', 'MATERIALIZED', 'REFRESH', 'EXPLAIN', 'ANALYZE', 'WITH',
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COALESCE', 'CAST', 'UPPER', 'LOWER',
    'TRIM', 'SUBSTRING', 'LENGTH', 'ROUND', 'NOW', 'DATE_TRUNC', 'DATE_DIFF',
    'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD', 'OVER', 'PARTITION',
    'PERCENTILE', 'PERCENTILE_APPROX', 'IS', 'NULL', 'BETWEEN', 'LIKE', 'EXISTS',
  ];

  private cachedDatasources: string[] = [];

  async loadDatasources() {
    try {
      const cfg = getConfig();
      const ds = await request(`${cfg.serverUrl}/api/fuse/datasources`, 'GET');
      this.cachedDatasources = (ds || []).map((d: any) => d.name || d.id);
    } catch { /* ignore */ }
  }

  provideCompletionItems(doc: vscode.TextDocument, pos: vscode.Position): vscode.CompletionItem[] {
    const items: vscode.CompletionItem[] = [];

    // SQL keywords
    for (const kw of this.keywords) {
      const item = new vscode.CompletionItem(kw, vscode.CompletionItemKind.Keyword);
      item.detail = 'SQL keyword';
      items.push(item);
    }

    // Datasources
    for (const ds of this.cachedDatasources) {
      const item = new vscode.CompletionItem(ds, vscode.CompletionItemKind.Module);
      item.detail = 'Datasource';
      items.push(item);
    }

    // PPL commands
    if (doc.languageId === 'fuseppl') {
      for (const cmd of ['source', 'search', 'where', 'fields', 'stats', 'sort', 'dedup', 'eval', 'head', 'top', 'rare', 'rename', 'parse', 'grok']) {
        const item = new vscode.CompletionItem(cmd, vscode.CompletionItemKind.Function);
        item.detail = 'PPL command';
        items.push(item);
      }
    }

    return items;
  }
}

// ── Activation ──

export function activate(context: vscode.ExtensionContext) {
  const dsProvider = new DatasourceProvider();
  const completionProvider = new FuseCompletionProvider();
  completionProvider.loadDatasources();

  context.subscriptions.push(
    vscode.commands.registerCommand('fuse.runQuery', runQuery),
    vscode.commands.registerCommand('fuse.explainQuery', explainQuery),
    vscode.commands.registerCommand('fuse.validateQuery', validateQuery),
    vscode.commands.registerCommand('fuse.showDatasources', () => dsProvider.refresh()),
    vscode.commands.registerCommand('fuse.insertQuery', (query: string) => {
      const editor = vscode.window.activeTextEditor;
      if (editor) { editor.insertSnippet(new vscode.SnippetString(query)); }
    }),

    vscode.window.registerTreeDataProvider('fuseDatasources', dsProvider),
    vscode.window.registerTreeDataProvider('fuseHistory', historyProvider),

    vscode.languages.registerCompletionItemProvider('fusesql', completionProvider),
    vscode.languages.registerCompletionItemProvider('fuseppl', completionProvider),
  );

  // Status bar
  const statusItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
  statusItem.text = '$(database) Fuse';
  statusItem.tooltip = 'Fuse Query Engine';
  statusItem.command = 'fuse.runQuery';
  statusItem.show();
  context.subscriptions.push(statusItem);
}

export function deactivate() {}
